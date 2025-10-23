namespace TntTemplateToFedexAddress
{
    using System.Collections.Concurrent;
    using System.IO;
    using System.Text;
    using System.Text.Json;

    internal class Program
    {
        internal static readonly char[] separator = [';'];
        internal static readonly string headerLine = "Nickname,FullName,FirstName,LastName,Title,Company,Department,AddressOne,AddressTwo,City,State,Zip,PhoneNumber,ExtensionNumber,FAXNumber,PagerNumber,MobilePhoneNumber,CountryCode,EmailAddress,VerifiedFlag,AcceptedFlag,ValidFlag,ResidentialFlag,CustomsIDEIN,ReferenceDescription,ServiceTypeCode,PackageTypeCode,CollectionMethodCode,BillCode,BillAccountNumber,DutyBillCode,DutyBillAccountNumber,CurrencyTypeCode,InsightIDNumber,GroundReferenceDescription,ShipmentNotificationRecipientEmail,RecipientEmailLanguage,RecipientEmailShipmentnotification,RecipientEmailExceptionnotification,RecipientEmailDeliverynotification,PartnerTypeCodes,NetReturnBillAccountNumber,CustomsIDTypeCode,AddressTypeCode,ShipmentNotificationSenderEmail,SenderEmailLanguage,SenderEmailShipmentnotification,SenderEmailExceptionnotification,SenderEmailDeliverynotification,RecipientEmailPickupnotification,SenderEmailPickupnotification,OpCoTypeCd,BrokerAccounttID,BrokerTaxID,DefaultBrokerID,RecipientEmailTenderednotification,SenderEmailTenderednotification,UserAccountNumber,DeliveryInstructions,EstimatedDeliveryFlag,SenderEstimatedDeliveryFlag,ShipmentNotificationSenderDeliveryChannel,ShipmentNotificationSenderMobileNo,ShipmentNotificationSenderMobileNoCountry,ShipmentNotificationSenderMobileNoLanguage";
        internal static readonly Dictionary<string, string> columnMap = new(StringComparer.OrdinalIgnoreCase)
        {
            // csvHeaderName => jsonKeyOrSpecial
            { "Nickname", "Part1" },          // use Part1 value
            { "FullName", "contactName" },      // map to receiver["company"]
            { "Company", "company" },      // map to receiver["contactName"]
            { "AddressOne", "addressLine1" },
            { "AddressTwo", "addressLine2" },
            { "City", "city" },
            { "State", "state" },
            { "Zip", "postcode" },
            { "PhoneNumber", "phoneNumber" },
            { "CountryCode", "country" },
            { "EmailAddress", "email" },
            { "VerifiedFlag", "DEFAULT:Y" },
            { "AcceptedFlag", "DEFAULT:N" },
            { "ValidFlag", "DEFAULT:Y" }
        };

        static async Task Main()
        {
            string folderPath = "C:\\Temp";
            string inputPath = folderPath + "\\000040562.txt";
            string outputPath = folderPath + "\\FedExAddressBook.csv";

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

                    const string DefaultPrefix = "DEFAULT:";
                    if (mapTarget.StartsWith(DefaultPrefix, StringComparison.OrdinalIgnoreCase))
                    {
                        string defaultValue = mapTarget[DefaultPrefix.Length..]; 
                        // may be empty
                        // Return defaultValue for every row (no need to inspect the row)
                        return new Func<SingleResult, string?>(_ => string.IsNullOrEmpty(defaultValue) ? null : defaultValue);
                    }

                    if (string.Equals(mapTarget, "Part1", StringComparison.OrdinalIgnoreCase))
                    {
                        // special case: use Part1 field
                        return new Func<SingleResult, string?>(row => row.Part1);
                    }
                    else
                    {
                        // Normal case: look up the mapped JSON key in receiver dictionary.
                        // Capture the mapped key in a local to avoid closure pitfalls.
                        string jsonKey = mapTarget;
                        return new Func<SingleResult, string?>(row => row.Receiver != null && row.Receiver.TryGetValue(jsonKey, out var v) ? v : null);
                    }
                })];

            // Write CSV sequentially to preserve order and avoid locking
            using var writer = new StreamWriter(outputPath, false, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));

            // Write header
            await writer.WriteLineAsync(string.Join(",", headerColumns.Select(CsvEscape)));

            // Write rows in original order
            for (int i = 0; i < results.Length; i++)
            {
                var row = results[i];

                // Use a StringBuilder per line to reduce allocations (optional optimization)
                var sb = new StringBuilder();

                for (int c = 0; c < getters.Length; c++)
                {
                    if (c > 0) sb.Append(','); // comma separator

                    string? rawValue = getters[c](row); // fast lookup
                    string escaped = CsvEscape(rawValue);
                    sb.Append(escaped);
                }

                await writer.WriteLineAsync(sb.ToString());
            }

            Console.WriteLine($"Wrote {results.Length} rows to {outputPath}");
        }

        // Helper moved to class level to avoid CS4033
        private static string CsvEscape(string? field)
        {
            if (field is null) return "";
            bool mustQuote = field.Contains('"') || field.Contains(',') || field.Contains('\n') || field.Contains('\r')
                             || field.StartsWith(' ') || field.EndsWith(' ');
            return !mustQuote ? field : "\"" + field.Replace("\"", "\"\"") + "\"";
        }
    }

    internal class SingleResult(int lineIndex, string part1, Dictionary<string, string?> receiver)
    {
        public int LineIndex { get; set; } = lineIndex;
        public string Part1 { get; set; } = part1;
        public Dictionary<string, string?> Receiver { get; set; } = receiver;
    }
}
