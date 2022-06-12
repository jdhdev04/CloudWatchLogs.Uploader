using Amazon.CloudWatchLogs;

using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;
using System;
using Microsoft.Extensions.Configuration;
using Serilog;
using System.Configuration;
using Amazon.CloudWatchLogs.Model;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Net;

namespace DOS.CloudWatchLogs.Uploader
{
    class Program
    {
        static string _awsProfileName = ConfigurationManager.AppSettings["AwsProfileName"];
        static string _folderToMoveLogsUploaded = ConfigurationManager.AppSettings["FolderToMoveLogsUploaded"];
        static ILogger _logger = null;

        static async System.Threading.Tasks.Task Main(string[] args)
        {
            //Create Logger and set configuration from JSON file.
            var configuration = new ConfigurationBuilder().AddJsonFile("SerilogConfig.json").Build();
            _logger = new LoggerConfiguration().ReadFrom.Configuration(configuration).CreateLogger();
            _logger.Information($" - START - {DateTime.Now}.");

            //Set AWS Credentials and CloudWatch Client
            var sharedFileCloudWatch = new SharedCredentialsFile();
            if (sharedFileCloudWatch.TryGetProfile(_awsProfileName, out CredentialProfile basicProfileCloudWatch) && AWSCredentialsFactory.TryGetAWSCredentials(basicProfileCloudWatch, sharedFileCloudWatch, out AWSCredentials awsCredentialsCloudWatch))
            {
                _logger.Information($" -    CREDENTIALS OK - {DateTime.Now}.");
                var client = new AmazonCloudWatchLogsClient(awsCredentialsCloudWatch, Amazon.RegionEndpoint.USEast1);
                //Lee de la config los logs que hay que subir
                var logsToParse = ParseLogsToUploadConfig();

                //Por cada log, busca dentro del path que esta configurado, que archivos hay para subir
                foreach (var log in logsToParse)
                {
                    string path = log.Key;
                    string logGroupName = log.Value;

                    _logger.Information($" -    PROCESANDO - {logGroupName}.");

                    if (Directory.Exists(path))
                    {
                        var dirInfo = new DirectoryInfo(path);
                        var logFiles = dirInfo.GetFiles().OrderBy(f => f.CreationTime).ToList();

                        _logger.Information($" -    FILES COUNT: {logFiles.Count}.");

                        foreach (FileInfo file in logFiles)
                        {
                            _logger.Information($" -        PROCESANDO - {file.Name}.");
                            try
                            {
                                //Por cada archivo creo un log group donde subo todas las lineas.
                                _logger.Information(file.FullName);
                                var lines = File.ReadAllLines(file.FullName).ToList();

                                string logStreamName = GetStreamName(file.CreationTime);
                                var createStreamResponse = await client.CreateLogStreamAsync(new CreateLogStreamRequest(logGroupName, logStreamName));

                                if (createStreamResponse.HttpStatusCode == HttpStatusCode.OK)
                                {
                                    _logger.Information($" -        LOG STREAM CREATED: {logStreamName}.");
                                    var listMsg = new List<InputLogEvent>();
                                    foreach (var item in lines)
                                    {
                                        var logItem = PrepareLogItem(item);
                                        listMsg.Add(new InputLogEvent() { Message = logItem, Timestamp = DateTime.Parse(item.Substring(0, item.IndexOf('[') - 1)) });
                                    }

                                    var request = new PutLogEventsRequest()
                                    {
                                        LogGroupName = logGroupName,
                                        LogStreamName = logStreamName
                                    };
                                    request.LogEvents = listMsg;

                                    var result = await client.PutLogEventsAsync(request);
                                    if (result.HttpStatusCode == HttpStatusCode.OK)
                                    {
                                        _logger.Information($" -        OK SUBIDA DE LOGS - {DateTime.Now}.");
                                    }
                                    else
                                    {
                                        _logger.Error($" -      ERROR AL SUBIR LOS LOGS - {DateTime.Now}.");
                                    }
                                    file.MoveTo(path + @"delete\" + file.Name);
                                }
                                else {
                                    _logger.Information($" -        LOG STREAM NOT CREATED: {logStreamName}.");
                                }
                            }
                            catch (Exception ex)
                            {
                                _logger.Fatal($" -      EXCEPTION - {DateTime.Now}. - " + ex.Message);
                            }
                        }
                    }
                    else
                    {
                        _logger.Error($" -  No existe el Directorio: {path}");
                    }
                }
            }
            else {
                _logger.Information($" -    CREDENTIALS FAIL - {DateTime.Now}.");
            }

            _logger.Information($" - END - {DateTime.Now}.");
        }

        static string PrepareLogItem(string item)
        {
            item = item.Remove(0, item.IndexOf('[') - 1);
            var result = "\"Level\"" + ": " + "\"" + item.Substring(item.IndexOf('[') + 1, item.IndexOf(']') - 2) + "\"";
            item = item.Remove(0, item.IndexOf(']') + 1);
            //item.Replace("{", "").Replace("}", "");
            item = item.Trim().Insert(1, result + ",");
            return item;            
        }

        static List<KeyValuePair<string, string>> ParseLogsToUploadConfig() 
        {
            var value = new List<KeyValuePair<string, string>>();
            var split1 = ConfigurationManager.AppSettings["LogsToUpload"].Split(';').ToList();

            _logger.Information($" - SE ENCONTRARON {split1.Count} LOGS CONFIGURADOS: ");

            foreach (var item in split1)
            {
                var split2 = item.Split('*');
                value.Add(new KeyValuePair<string, string>(split2[0].ToString(), split2[1].ToString()));
                _logger.Information($" - LOG: {split2[1]} PATH: {split2[0]}");
            }

            return value;
        }

        static string GetStreamName(DateTime creationTime)
        {
            int num = new Random().Next(10000);
            return creationTime.Year.ToString()
                          + (creationTime.Month.ToString().Length == 1 ? "0" + creationTime.Month.ToString() : creationTime.Month.ToString())
                          + (creationTime.Day.ToString().Length == 1 ? "0" + creationTime.Day.ToString() : creationTime.Day.ToString())
                          + "_"
                          + (creationTime.Hour.ToString().Length == 1 ? "0" + creationTime.Hour.ToString() : creationTime.Hour.ToString())
                          + (creationTime.Minute.ToString().Length == 1 ? "0" + creationTime.Minute.ToString() : creationTime.Minute.ToString())
                          + (creationTime.Second.ToString().Length == 1 ? "0" + creationTime.Second.ToString() : creationTime.Second.ToString())
                          + (creationTime.Millisecond.ToString().Length == 1 ? "0" + creationTime.Millisecond.ToString() : creationTime.Millisecond.ToString())
                          + "_" 
                          + num.ToString()
                          ;
        }

        static string GetExecutionId()
        {            
            return DateTime.Now.Year.ToString()
                      + (DateTime.Now.Month.ToString().Length == 1 ? "0" + DateTime.Now.Month.ToString() : DateTime.Now.Month.ToString())
                      + (DateTime.Now.Day.ToString().Length == 1 ? "0" + DateTime.Now.Day.ToString() : DateTime.Now.Day.ToString())
                      + "_"
                      + (DateTime.Now.Hour.ToString().Length == 1 ? "0" + DateTime.Now.Hour.ToString() : DateTime.Now.Hour.ToString())
                      + (DateTime.Now.Minute.ToString().Length == 1 ? "0" + DateTime.Now.Minute.ToString() : DateTime.Now.Minute.ToString())
                      + (DateTime.Now.Second.ToString().Length == 1 ? "0" + DateTime.Now.Second.ToString() : DateTime.Now.Second.ToString())
                      + (DateTime.Now.Millisecond.ToString().Length == 1 ? "0" + DateTime.Now.Millisecond.ToString() : DateTime.Now.Millisecond.ToString())
                      ;
        }


    }
}
