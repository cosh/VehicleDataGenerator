using System.Globalization;
using System.IO.Compression;
using System.Text;

namespace VehicleDataGenerator
{
    internal class GeneratorTask
    {
        private readonly List<string> vins;

        private readonly int rowsPerBatch;

        private readonly string baseOutputDir;

        private readonly List<KeyValuePair<string, string>> columns;

        private readonly DateTime start;

        private readonly DateTime end;

        private readonly string day;

        private const string SEPARATOR = ",";

        private string fileName;

        private string filenameLastPart;

        private readonly double _lon_min;

        private readonly double _lon_max;

        private readonly double _lat_min;

        private readonly double _lat_max;

        public List<string> Vins => vins;

        public int RowsPerBatch => rowsPerBatch;

        public string BaseOutputDir => baseOutputDir;

        public DateTime Start => start;

        public DateTime End => end;

        public string Day => day;

        public string FileName => fileName;

        public string FilenameLastPart => filenameLastPart;

        public GeneratorTask(int myDiffDays, DateTime myEndDate, List<string> myVins, int rowsPerBatch, string baseOutputDir, List<KeyValuePair<string, string>> columns,
            double lon_min, double lon_max, double lat_min, double lat_max)
        {
            vins = myVins;
            this.rowsPerBatch = rowsPerBatch;
            this.baseOutputDir = baseOutputDir;
            this.columns = columns;
            DateTime endOfDataGenerator = myEndDate.AddDays(-1 * myDiffDays);
            start = new DateTime(endOfDataGenerator.Year, endOfDataGenerator.Month, endOfDataGenerator.Day, 0, 0, 0);
            end = new DateTime(endOfDataGenerator.Year, endOfDataGenerator.Month, endOfDataGenerator.Day, 23, 59, 59);
            day = end.ToString("yyyyMMdd");
            _lon_max = lon_max;
            _lon_min = lon_min;
            _lat_min = lat_min;
            _lat_max = lat_max;
        }

        public void Run()
        {
            Random prng = new Random();
            string fileGuid = Guid.NewGuid().ToString();
            string directory = baseOutputDir.Remove(0, 6) + "\\" + day;
            Directory.CreateDirectory(directory);
            filenameLastPart = fileGuid + ".csv.gz";
            fileName = Path.Combine(directory, filenameLastPart);
            StringBuilder stringbuilder = new StringBuilder();
            //adding headers
            stringbuilder.AppendLine(columns.Select((KeyValuePair<string, string> _) => _.Key).Aggregate((string a, string b) => a + "," + b));
            
            for (int i = 0; i < rowsPerBatch; i++)
            {
                var ts = start.AddSeconds(prng.Next(0, 86400));

                string tsString = start.AddSeconds(prng.Next(0, 86400)).ToString("s");
                stringbuilder.AppendLine(columns.Select((KeyValuePair<string, string> _) => GenerateColumn(_.Key, _.Value, prng, tsString, ts)).Aggregate((string a, string b) => a + "," + b));
            }
            using FileStream outFile = File.Create(fileName);
            using GZipStream compress = new GZipStream(outFile, CompressionMode.Compress);
            using StreamWriter writer = new StreamWriter(compress);
            writer.Write(stringbuilder.ToString());
        }

        private string GenerateColumn(string columnName, string columnType, Random prng, string tsString, DateTime ts)
        {
            string result = "";
            switch (columnName)
            {
                case "vin":
                    result = vins[prng.Next(0, vins.Count)];
                    break;
                case "ts":
                    result = tsString;
                    break;
                case "backend_ts":
                    result = ts.AddSeconds(prng.Next(0, 30)).ToString("s");
                    break;
                case "traceId":
                    result = Guid.NewGuid().ToString();
                    break;
                case "gps_lon":
                    result = GetPseudoDoubleWithinRange(_lon_min, _lon_max, prng).ToString(new CultureInfo("en-US")).Substring(0, 8);
                    break;
                case "gps_lat":
                    result = GetPseudoDoubleWithinRange(_lat_min, _lat_max, prng).ToString(new CultureInfo("en-US")).Substring(0, 8);
                    break;
                default:
                    if (prng.Next() > 1073741823)
                    {
                        result = GenerateBasedOnType(columnName, columnType, prng);
                    }
                    break;
            }
            return result;
        }

        private  double GetPseudoDoubleWithinRange(double lowerBound, double upperBound, Random random)
        {
            var rDouble = random.NextDouble();
            var rRangeDouble = rDouble * (upperBound - lowerBound) + lowerBound;
            return rRangeDouble;
        }

        private string GenerateBasedOnType(string columnName, string columnType, Random prng)
        {
            string result = "";
            switch (columnType)
            {
                case "BOOLEAN":
                    result = (prng.Next() > 1073741823).ToString();
                    break;
                case "SMALLINT":
                    result = prng.Next(0, 10000).ToString();
                    break;
                case "DECIMAL":
                    result = (prng.NextDouble() * 100.0).ToString(new CultureInfo("en-US")).Substring(0, 6);
                    break;
                case "SMALLDECIMAL":
                    result = (prng.NextDouble() * 50.0).ToString(new CultureInfo("en-US")).Substring(0, 6);
                    break;
                case "BIGDECIMAL":
                    result = (prng.NextDouble() * 5000.0).ToString(new CultureInfo("en-US")).Substring(0, 6);
                    break;
                case "TINYINT":
                    result = prng.Next(0, 5).ToString();
                    break;
                case "INT":
                    result = prng.Next(0, 1000000).ToString();
                    break;
                case "VARCHAR":
                    result = "A";
                    break;
            }
            return result;
        }
    }
}