namespace TntTemplateToFedexAddress
{
    using System.Collections.Concurrent;
    using System.IO;
    using System.Text;
    using System.Text.Json;

    internal class Program
    {
        private static readonly char[] separator = [';'];
        private static readonly string headerLine = "Nickname,FullName,FirstName,LastName,Title,Company,Department,AddressOne,AddressTwo,City,State,Zip,PhoneNumber,ExtensionNumber,FAXNumber,PagerNumber,MobilePhoneNumber,CountryCode,EmailAddress,VerifiedFlag,AcceptedFlag,ValidFlag,ResidentialFlag,CustomsIDEIN,ReferenceDescription,ServiceTypeCode,PackageTypeCode,CollectionMethodCode,BillCode,BillAccountNumber,DutyBillCode,DutyBillAccountNumber,CurrencyTypeCode,InsightIDNumber,GroundReferenceDescription,ShipmentNotificationRecipientEmail,RecipientEmailLanguage,RecipientEmailShipmentnotification,RecipientEmailExceptionnotification,RecipientEmailDeliverynotification,PartnerTypeCodes,NetReturnBillAccountNumber,CustomsIDTypeCode,AddressTypeCode,ShipmentNotificationSenderEmail,SenderEmailLanguage,SenderEmailShipmentnotification,SenderEmailExceptionnotification,SenderEmailDeliverynotification,RecipientEmailPickupnotification,SenderEmailPickupnotification,OpCoTypeCd,BrokerAccounttID,BrokerTaxID,DefaultBrokerID,RecipientEmailTenderednotification,SenderEmailTenderednotification,UserAccountNumber,DeliveryInstructions,EstimatedDeliveryFlag,SenderEstimatedDeliveryFlag,ShipmentNotificationSenderDeliveryChannel,ShipmentNotificationSenderMobileNo,ShipmentNotificationSenderMobileNoCountry,ShipmentNotificationSenderMobileNoLanguage";
        private static readonly Dictionary<string, (string, bool, string, Func<string?, string?>?)> columnMap = new(StringComparer.OrdinalIgnoreCase)
        {
            // value tuple: (jsonKey, useDefaultFlag, defaultValue, normalizationFunc)
            // csvHeaderName => jsonKeyOrSpecial
            { "Nickname", ("Part1", false, "", null) },            // use Part1 value
            { "FullName", ("contactName", false, "", null) },      // map to receiver["company"]
            { "Company", ("company", false, "", null) },           // map to receiver["contactName"]
            { "AddressOne", ("addressLine1", false, "", null) },
            { "AddressTwo", ("addressLine2", false, "", null) },
            { "City", ("city", false, "", null) },
            { "State", ("state", false, "", null) },
            { "Zip", ("postcode", false, "", null) },
            { "PhoneNumber", ("phoneNumber", false, "", NormalizePhoneNumbers) },
            { "CountryCode", ("country", false, "", null) },
            { "EmailAddress", ("email", false, "", null) },
            { "VerifiedFlag", ("", true, "Y", null) },
            { "AcceptedFlag", ("", true, "N", null) },
            { "ValidFlag", ("", true, "Y", null) }
        };
        private static readonly char[] s_singleMap = CreateSingleMap();
        private static readonly bool skipFirstLine = true;

        static async Task Main()
        {
            string folderPath = "C:\\Temp";
            string inputPath = folderPath + "\\000040562.txt";
            string outputPath = folderPath + "\\FedExAddressBook.csv";

            //Dictionary<int, char> charDict = new Dictionary<int, char>();

            //for (int i = 32; i < 128; i++)
            //{
            //    charDict[i] = (char)i;
            //}

            //using var file = new StreamWriter(folderPath + "\\charmap.txt", false, Encoding.UTF8);
            //foreach (var kvp in charDict.OrderBy(s => s.Key))
            //{
            //    file.WriteLine($"{kvp.Key}\t\tmap['{kvp.Value}'] = ' ';");
            //}

            //return;

            // Provide the CSV header line here (comma-separated).
            // Include "Part1" as a special column name to get the value from parts[1].

            var headerColumns = headerLine.Split(',')
                                          .Select(h => h.Trim())
                                          .ToArray();

            // Read all lines asynchronously (OK for ~4000 lines)
            string[] lines = await File.ReadAllLinesAsync(inputPath, Encoding.UTF8);

            // Prepare a structure to store results in original order
            var results = new SingleResult[lines.Length];

            int maxDop = Math.Max(1, Environment.ProcessorCount);
            var partitioner = Partitioner.Create(0, lines.Length);

            ParallelOptions pOptions = new() { MaxDegreeOfParallelism = maxDop };

            Parallel.ForEach(partitioner, pOptions, range =>
            {
                for (int i = range.Item1; i < range.Item2; i++)
                {
                    string line = lines[i];
                    if (string.IsNullOrWhiteSpace(line))
                    {
                        results[i] = new SingleResult(i, string.Empty, new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase));
                        continue;
                    }

                    // Split into at most 3 parts
                    string[] parts = line.Split(separator, 3);
                    Array.Resize(ref parts, 3);
                    for (int k = 0; k < parts.Length; k++)
                        parts[k] = parts[k]?.Trim() ?? string.Empty;

                    string part1 = parts[1]; // keep this as an output field

                    string raw = parts[2];

                    // Remove surrounding quotes if present
                    if (raw.Length >= 2 && raw[0] == '"' && raw[^1] == '"')
                        raw = raw[1..^1];

                    // Replace doubled quotes with a single quote
                    raw = raw.Replace("\"\"", "\"");

                    var receiverDict = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);

                    try
                    {
                        using JsonDocument doc = JsonDocument.Parse(raw);
                        if (doc.RootElement.TryGetProperty("receiver", out JsonElement receiverElem) &&
                            receiverElem.ValueKind == JsonValueKind.Object)
                        {
                            foreach (var prop in receiverElem.EnumerateObject())
                            {
                                string key = prop.Name;
                                JsonElement val = prop.Value;

                                string? valueString = val.ValueKind switch
                                {
                                    JsonValueKind.String => val.GetString(),
                                    JsonValueKind.Number => val.GetRawText(),
                                    JsonValueKind.True => "true",
                                    JsonValueKind.False => "false",
                                    JsonValueKind.Null => null,
                                    _ => val.GetRawText()
                                };

                                receiverDict[key] = valueString;
                            }
                        }
                        // else leave receiverDict empty (no receiver)
                    }
                    catch (JsonException)
                    {
                        // On parse error we keep an empty dictionary; optionally log or collect errors
                    }

                    results[i] = new SingleResult(i, part1, receiverDict);
                }
            });

            Func<SingleResult, string?>[] getters =
                [.. headerColumns.Select(col =>
                {
                    if (!columnMap.TryGetValue(col, out var mapTarget))
                    {
                        // If no mapping provided, default to empty value
                        return new Func<SingleResult, string?>(row => null);
                    }

                    // unpack for clarity
                    string jsonKey = mapTarget.Item1;
                    bool useDefault = mapTarget.Item2;
                    string defaultValue = mapTarget.Item3;
                    Func<string?, string?>? normalize = mapTarget.Item4;

                    // helper to apply normalization (handle null)
                    string? ApplyNormalize(string? v) => normalize != null ? normalize(v) : v;

                    if (useDefault)
                    {
                        if (string.IsNullOrEmpty(jsonKey))
                        {
                            // always return default (or null if default is empty), with normalization
                            return new Func<SingleResult, string?>(_ => ApplyNormalize(string.IsNullOrEmpty(defaultValue) ? null : defaultValue));
                        }
                        else
                        {
                            // prefer receiver value, but fall back to default when missing/null/empty
                            return new Func<SingleResult, string?>(row =>
                            {
                                if (row.Receiver != null && row.Receiver.TryGetValue(jsonKey, out var v) && !string.IsNullOrEmpty(v))
                                {
                                    return ApplyNormalize(v);
                                }
                                return ApplyNormalize(string.IsNullOrEmpty(defaultValue) ? null : defaultValue);
                            });
                        }
                    }

                    if (string.Equals(jsonKey, "Part1", StringComparison.OrdinalIgnoreCase))
                    {
                        // special case: use Part1 field (normalized)
                        return new Func<SingleResult, string?>(row => ApplyNormalize(row.Part1));
                    }
                    else
                    {
                        // Normal case: look up the mapped JSON key in receiver dictionary.
                        return new Func<SingleResult, string?>(row =>
                        {
                            if (row.Receiver != null && row.Receiver.TryGetValue(jsonKey, out var v))
                            {
                                return ApplyNormalize(v);
                            }
                            return ApplyNormalize(null);
                        });
                    }
                })];

            // Write CSV sequentially to preserve order and avoid locking
            using var writer = new StreamWriter(outputPath, false, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));

            // Write header
            await writer.WriteLineAsync(string.Join(",", headerColumns.Select(a => CsvEscape(a))));

            // Write rows in original order
            for (int i = skipFirstLine ? 1 : 0; i < results.Length; i++)
            {
                var row = results[i];
                var countryGetter = getters!.FirstOrDefault(g => g.Method.Name.Contains("CountryCode"));
                var country = countryGetter != null ? countryGetter(row) : null;
                bool specialCountry = string.Equals(country, "DE", StringComparison.OrdinalIgnoreCase) ||
                                      string.Equals(country, "AT", StringComparison.OrdinalIgnoreCase) ||
                                      string.Equals(country, "CH", StringComparison.OrdinalIgnoreCase);

                // Use a StringBuilder per line to reduce allocations (optional optimization)
                var sb = new StringBuilder();

                for (int c = 0; c < getters!.Length; c++)
                {
                    if (c > 0) sb.Append(','); // comma separator

                    string? rawValue = getters[c](row); // fast lookup
                    string normalized = CsvEscape(rawValue, specialCountry);
                    sb.Append(normalized);
                }

                await writer.WriteLineAsync(sb.ToString());
            }

            Console.WriteLine($"Wrote {results.Length} rows to {outputPath}");
        }

        // Helper moved to class level to avoid CS4033
        private static string CsvEscape(string? origin, bool specialCountry = false)
        {
            return string.IsNullOrEmpty(origin) ? "" : Normalize(origin, specialCountry).Trim(' ');
        }

        internal static string NormalizePhoneNumbers(string? src)
        {
            if (string.IsNullOrEmpty(src)) return string.Empty;

            // count digits first
            int count = 0;
            for (int i = 0; i < src.Length; i++)
            {
                char c = src[i];
                if (c >= '0' && c <= '9') count++;
            }
            if (count == src.Length) return src; // nothing to remove
            if (count == 0) return string.Empty;

            // create result of exact length and fill it
            return string.Create(count, src, (span, s) =>
            {
                int j = 0;
                for (int i = 0; i < s.Length; i++)
                {
                    char c = s[i];
                    if (c >= '0' && c <= '9') span[j++] = c;
                }
            });
        }

        // Public API
        // origin: input string (Latin-1 + Latin-2). specialCountry: expand Ä/ä/Ö/ö/Ü/ü to Ae/Oe/Ue respectively.
        public static string Normalize(string origin, bool specialCountry)
        {
            var src = origin.AsSpan();

            // First pass: determine exact output length
            int outLen = 0;
            for (int i = 0; i < src.Length; i++)
            {
                char c = src[i];

                var multi = Latin1Normalizer.TryMulti(c, specialCountry);
                outLen += (multi != null) ? multi.Length : 1;
            }

            // Second pass: fill into a stack-allocated buffer
            Span<char> buffer = stackalloc char[outLen];
            int pos = 0;

            for (int i = 0; i < src.Length; i++)
            {
                char c = src[i];
                var multi = Latin1Normalizer.TryMulti(c, specialCountry);
                if (multi != null)
                {
                    for (int j = 0; j < multi.Length; j++)
                        buffer[pos++] = multi[j];
                }
                else
                {
                    char mapped = c switch
                    {
                        (char)8217 => '\'',
                        (char)8211 => '-',
                        _ => s_singleMap[c]
                    };
                    buffer[pos++] = mapped;
                }
            }

            // Create the final string from the buffer
            return new string(buffer[..outLen]);
        }

        private static char[] CreateSingleMap()
        {
            var map = new char[383];
            // Initialize to identity
            for (int i = 0; i < map.Length; i++) map[i] = (char)i;

            // Common diacritics to ASCII equivalents (Latin-1 → ASCII)
            map['À'] = 'A'; map['Á'] = 'A'; map['Â'] = 'A'; map['Ã'] = 'A'; map['Ä'] = 'A'; map['Å'] = 'A'; map['Æ'] = 'A';
            map['Ç'] = 'C';
            map['È'] = 'E'; map['É'] = 'E'; map['Ê'] = 'E'; map['Ë'] = 'E';
            map['Ì'] = 'I'; map['Í'] = 'I'; map['Î'] = 'I'; map['Ï'] = 'I';
            map['Ð'] = 'D';
            map['Ñ'] = 'N';
            map['Ò'] = 'O'; map['Ó'] = 'O'; map['Ô'] = 'O'; map['Õ'] = 'O'; map['Ö'] = 'O'; map['Ø'] = 'O';
            map['Ù'] = 'U'; map['Ú'] = 'U'; map['Û'] = 'U'; map['Ü'] = 'U';
            map['Ý'] = 'Y';
            map['à'] = 'a'; map['á'] = 'a'; map['â'] = 'a'; map['ã'] = 'a'; map['ä'] = 'a'; map['å'] = 'a'; map['æ'] = 'a'; map['ç'] = 'c';
            map['è'] = 'e'; map['é'] = 'e'; map['ê'] = 'e'; map['ë'] = 'e';
            map['ì'] = 'i'; map['í'] = 'i'; map['î'] = 'i'; map['ï'] = 'i';
            map['ð'] = 'o';
            map['ñ'] = 'n';
            map['ò'] = 'o'; map['ó'] = 'o'; map['ô'] = 'o'; map['õ'] = 'o'; map['ö'] = 'o'; map['ø'] = 'o';
            map['ù'] = 'u'; map['ú'] = 'u'; map['û'] = 'u'; map['ü'] = 'u';
            map['ý'] = 'y'; map['ÿ'] = 'y';
            map['Ā'] = 'A'; map['ā'] = 'a';
            map['Ă'] = 'A'; map['ă'] = 'a';
            map['Ą'] = 'A'; map['ą'] = 'a';
            map['Ć'] = 'C'; map['ć'] = 'c';
            map['Ĉ'] = 'C'; map['ĉ'] = 'c';
            map['Ċ'] = 'C'; map['ċ'] = 'c';
            map['Č'] = 'C'; map['č'] = 'c';
            map['Ď'] = 'D'; map['ď'] = 'd';
            map['Đ'] = 'D'; map['đ'] = 'd';
            map['Ē'] = 'E'; map['ē'] = 'e';
            map['Ĕ'] = 'E'; map['ĕ'] = 'e';
            map['Ė'] = 'E'; map['ė'] = 'e';
            map['Ę'] = 'E'; map['ę'] = 'e';
            map['Ě'] = 'E'; map['ě'] = 'e';
            map['Ĝ'] = 'G'; map['ĝ'] = 'g';
            map['Ğ'] = 'G'; map['ğ'] = 'g';
            map['Ġ'] = 'G'; map['ġ'] = 'g';
            map['Ģ'] = 'G'; map['ģ'] = 'g';
            map['Ĥ'] = 'H'; map['ĥ'] = 'h';
            map['Ħ'] = 'H'; map['ħ'] = 'h';
            map['Ĩ'] = 'I'; map['ĩ'] = 'i';
            map['Ī'] = 'I'; map['ī'] = 'i';
            map['Ĭ'] = 'I'; map['ĭ'] = 'i';
            map['Į'] = 'I'; map['į'] = 'i';
            map['İ'] = 'I'; map['ı'] = 'i';
            map['Ĵ'] = 'J'; map['ĵ'] = 'j';
            map['Ķ'] = 'K'; map['ķ'] = 'k';
            map['Ĺ'] = 'L'; map['ĺ'] = 'l';
            map['Ļ'] = 'L'; map['ļ'] = 'l';
            map['Ľ'] = 'L'; map['ľ'] = 'l';
            map['Ŀ'] = 'L'; map['ŀ'] = 'l';
            map['Ł'] = 'L'; map['ł'] = 'l';
            map['Ń'] = 'N'; map['ń'] = 'n';
            map['Ņ'] = 'N'; map['ņ'] = 'n';
            map['Ň'] = 'N'; map['ň'] = 'n';
            map['Ō'] = 'O'; map['ō'] = 'o';
            map['Ŏ'] = 'O'; map['ŏ'] = 'o';
            map['Ő'] = 'O'; map['ő'] = 'o';
            map['Ŕ'] = 'R'; map['ŕ'] = 'r';
            map['Ŗ'] = 'R'; map['ŗ'] = 'r';
            map['Ř'] = 'R'; map['ř'] = 'r';
            map['Ś'] = 'S'; map['ś'] = 's';
            map['Ŝ'] = 'S'; map['ŝ'] = 's';
            map['Ş'] = 'S'; map['ş'] = 's';
            map['Š'] = 'S'; map['š'] = 's';
            map['Ţ'] = 'T'; map['ţ'] = 't';
            map['Ť'] = 'T'; map['ť'] = 't';
            map['Ŧ'] = 'T'; map['ŧ'] = 't';
            map['Ũ'] = 'U'; map['ũ'] = 'u';
            map['Ū'] = 'U'; map['ū'] = 'u';
            map['Ŭ'] = 'U'; map['ŭ'] = 'u';
            map['Ů'] = 'U'; map['ů'] = 'u';
            map['Ű'] = 'U'; map['ű'] = 'u';
            map['Ų'] = 'U'; map['ų'] = 'u';
            map['Ŵ'] = 'W'; map['ŵ'] = 'w';
            map['Ŷ'] = 'Y'; map['ŷ'] = 'y';
            map['Ÿ'] = 'Y'; map['Ź'] = 'Z';
            map['ź'] = 'z'; map['Ż'] = 'Z';
            map['ż'] = 'z'; map['Ž'] = 'Z'; map['ž'] = 'z';
            map['"'] = ' ';
            map[','] = ' ';
            map['\r'] = ' ';
            map['\n'] = ' ';
            // Keep other characters as-is (pass-through)
            return map;
        }
    }

    internal class SingleResult(int lineIndex, string part1, Dictionary<string, string?> receiver)
    {
        public int LineIndex { get; set; } = lineIndex;
        public string Part1 { get; set; } = part1;
        public Dictionary<string, string?> Receiver { get; set; } = receiver;
    }

    internal static class Latin1Normalizer
    {
        // Multi-char replacements. Returns null if no multi-char replacement is needed.
        internal static string? TryMulti(char c, bool specialCountry)
        {
            return c switch
            {
                'Æ' => "Ae",
                'æ' => "ae",
                'ß' => "ss",
                'Þ' => "Th",
                'þ' => "th",
                // Special country expansions (only when requested)
                'Ä' => specialCountry ? "Ae" : null,
                'ä' => specialCountry ? "ae" : null,
                'Ö' => specialCountry ? "Oe" : null,
                'ö' => specialCountry ? "oe" : null,
                'Ü' => specialCountry ? "Ue" : null,
                'ü' => specialCountry ? "ue" : null,
                _ => null,
            };
        }
    }
}
