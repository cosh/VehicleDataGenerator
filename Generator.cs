using Azure.Core;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Resources;
using System.Text;
using System.Threading.Tasks;

namespace VehicleDataGenerator
{
    public class Generator
    {
        private readonly IConfiguration _config;
        private readonly ILogger<Generator> _logger;

        public Generator(IConfigurationRoot config, ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<Generator>();
            _config = config;
        }

        public async Task Run()
        {
            DateTime endDate = _config.GetValue<DateTime>("endDate");
            int countOfDays = _config.GetValue<int>("countOfDays");
            int countOfCars = _config.GetValue<int>("countOfCars");
            int rowsPerBatch = _config.GetValue<int>("rowsPerBatch");
            string toBeGenerated = _config.GetValue<string>("toBeGenerated");
            int parallelTaskCount = _config.GetValue<int>("parallelTaskCount");
            string vinPrefix = _config.GetValue<string>("vinPrefix");

            string storageConnectionString = _config.GetValue<string>("storageConnectionString");
            string containerName = _config.GetValue<string>("containerName");
            string baseOutputDir = Path.GetDirectoryName(Assembly.GetExecutingAssembly().GetName().CodeBase);
            List<string> vins = GenerateVins(countOfCars, vinPrefix);
            List<KeyValuePair<string, string>> columns = (from _ in toBeGenerated.Split(",")
                                                          where _.Contains("|")
                                                          select _).Select(delegate (string aColumnString)
                                                          {
                                                              string[] array = aColumnString.Split("|");
                                                              return new KeyValuePair<string, string>(array[0], array[1]);
                                                          }).ToList();
            BlobClientOptions options = new BlobClientOptions();
            options.Retry.Mode = RetryMode.Exponential;
            options.Retry.Delay = TimeSpan.FromSeconds(30.0);
            options.Retry.MaxRetries = 4;
            options.Retry.NetworkTimeout = TimeSpan.FromSeconds(30.0);
            BlobServiceClient blobServiceClient = new BlobServiceClient(storageConnectionString, options);
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);
            Task[] taskArray = new Task[parallelTaskCount];
            int counter = 0;
            for (int i = 0; i < countOfDays; i++)
            {
                taskArray[counter] = Task.Factory.StartNew(delegate (object obj)
                {
                    GeneratorTask generatorTask = obj as GeneratorTask;
                    generatorTask.Run();
                    _logger.LogInformation("Finished GENERATING day " + generatorTask.Day);
                    return generatorTask;
                }, new GeneratorTask(i, endDate, vins, rowsPerBatch, baseOutputDir, columns)).ContinueWith(delegate (Task<GeneratorTask> x)
                {
                    GeneratorTask result2 = x.Result;
                    _logger.LogInformation("Started uploading day " + result2.Day);
                    string blobName = result2.Day + "\\" + result2.FilenameLastPart;
                    BlobClient blobClient = containerClient.GetBlobClient(blobName);
                    using (FileStream content = File.OpenRead(result2.FileName))
                    {
                        try
                        {
                            blobClient.Upload(content, overwrite: false, CancellationToken.None);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError("Something funky happened during blob upload. Don't care and continue. Exception message: " + ex.Message);
                        }
                    }
                    _logger.LogInformation("Finished UPLOAD of day " + result2.Day + " and uploaded file " + result2.FileName);
                    return result2;
                }).ContinueWith(delegate (Task<GeneratorTask> x)
                {
                    GeneratorTask result = x.Result;
                    _logger.LogInformation("Deleting " + result.Day);
                    File.Delete(result.FileName);
                    _logger.LogInformation("Finished CLEANUP day " + result.Day);
                });
                counter++;
                if (counter % parallelTaskCount == 0)
                {
                    WaitForTasks(taskArray);
                    taskArray = new Task[parallelTaskCount];
                    counter = 0;
                }
            }
            if (counter > 0)
            {
                WaitForTasks(taskArray);
            }
        }

        private void WaitForTasks(Task[] myTaskArray)
        {
            try
            {
                Task.WaitAll(myTaskArray.Where((Task _) => _ != null).ToArray());
            }
            catch (Exception e)
            {
                _logger.LogError("Something funky happened. Don't care and continue. Exception message: " + e.Message);
            }
        }

        private List<string> GenerateVins(int countOfCars, String vinPrefix)
        {
            List<string> result = new List<string>();
            Random prng = new Random();
            for (int i = 0; i < countOfCars; i++)
            {
                result.Add(vinPrefix + prng.Next(0, countOfCars));
            }
            return result;
        }
    }
}
