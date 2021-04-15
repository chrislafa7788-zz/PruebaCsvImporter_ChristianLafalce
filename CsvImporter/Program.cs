using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using FastMember;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Auth;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage.DataMovement;
using Microsoft.Extensions.Configuration;
using Serilog;


namespace CsvImporter
{

    public class Program
    {
        static public IConfigurationRoot Configuration { get; set; }
        static void Main(string[] args)
        {
            
                

            string path= Path.GetDirectoryName(Assembly.GetEntryAssembly().Location);
             path = Path.Combine(path, "..", "..", "..");
            var builder = new ConfigurationBuilder()
                    .SetBasePath(path)
                    .AddJsonFile("appsettings.json");
            Configuration = builder.Build();
            

            crearArchivoLogeo();
            Log.Information("/////////// Ejecutando el script de BD ///////////");
            ejecutarScriptBD();


            

            //test
           
            //var cloudBlockBlob = new CloudBlockBlob(new Uri(Configuration.GetSection("URLS_ToGetTheInfo")["blob_web_1M"]));
            //var cloudBlockBlob = new CloudBlockBlob(new Uri(Configuration.GetSection("URLS_ToGetTheInfo")["blob_web_test_5ok_4repetidos"]));
            //var cloudBlockBlob = new CloudBlockBlob(new Uri(Configuration.GetSection("URLS_ToGetTheInfo")["blob_web_stock103mil"]));
            //var cloudBlockBlob = new CloudBlockBlob(new Uri(Configuration.GetSection("URLS_ToGetTheInfo")["blob_web_Stock3M"]));
            var cloudBlockBlob = new CloudBlockBlob(new Uri(Configuration.GetSection("URLS_ToGetTheInfo")["blob_web_Stock"]));




            TransferManager.Configurations.ParallelOperations = 5;          

            DownloadOptions options = new DownloadOptions();
            SingleTransferContext context = new SingleTransferContext();
            CancellationTokenSource cancellationSource = new CancellationTokenSource();
          
            string localFile = Path.GetDirectoryName(Assembly.GetEntryAssembly().Location) +"\\"+ "STOCK_" + DateTime.Now.ToString("yyyy_dd_M_HH_mm_ss") + ".CSV";


            Log.Information("Comenzando descarga...");

            Stopwatch stopwatchgral = new Stopwatch();
            stopwatchgral.Start();

            Stopwatch stopwatchTarea = new Stopwatch();
            stopwatchTarea.Start();
            
            try
            {
                DownloadOptions optionsWithDisableContentMD5Validation1 = new DownloadOptions() { DisableContentMD5Validation = true };
                context.ProgressHandler = new Progress<TransferStatus>((progress) =>
                {
                    if (progress.BytesTransferred > 0)
                    {
                        Log.Information("Mb descargados {0:F}\n", (progress.BytesTransferred / 1024) / 1024);
                        stopwatchTarea.Restart();
                    }
                });
                
                var task = TransferManager.DownloadAsync(cloudBlockBlob, localFile, optionsWithDisableContentMD5Validation1, context, cancellationSource.Token);

                evaluacionDeCaracteresDeErrorYretry(task,  stopwatchTarea);

                task.Wait();
            }
            catch (Exception e){               

                Log.Error("\nLa transferencia fue cancelada: {0}", e.Message);
                cancellationSource.Cancel();
            }
            
            stopwatchTarea.Stop();            

            if (cancellationSource.IsCancellationRequested)
            {
                Console.WriteLine("\nLa transferencia será reanudada en 3 segundos...");
                Thread.Sleep(3000);

                retryTransferencia(context, cloudBlockBlob, localFile);

            }
            
           
            System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);

            List<string> records = new List<string>();

            Log.Information("\n *******Operacion de transferencia completada con exito " + stopwatchgral.Elapsed.TotalSeconds + " segundos ******");
           

            using (StreamReader reader = new StreamReader(localFile, Encoding.GetEncoding(1251), true))
            {
                //Salteo la primera linae del encabezado
                if (!reader.EndOfStream) { reader.ReadLine(); }
                string file = reader.ReadToEnd();
                records = new List<string>(file.Split('\n'));
                Log.Information("El largo del archivo es: " + (reader.BaseStream.Length / 1024) / 1024 + " mb");

            }

            stopwatchgral.Stop();
             
            Log.Information("El tiempo en seg tomado es: : " + (stopwatchgral.ElapsedMilliseconds / 1000) + " segundos");
            Log.Information("El tiempo en min tomado es: : " + (stopwatchgral.ElapsedMilliseconds / 1000) / 60 + " minutos");




            stopwatchgral.Reset();
            stopwatchgral.Start();

            List<SaleStock> stcklist = new List<SaleStock>();

            Log.Information("Comenzando a copiar el archivo a memoria");

            foreach (string record in records)
            {   
                if (record != "") { 
                SaleStock stck = new SaleStock();
                string[] textpart = record.Split(';');
                stck.PointOfSale = textpart[0];
                stck.Product = textpart[1];
                stck.Date = Convert.ToDateTime(textpart[2]);
                stck.Stock = Convert.ToInt32(textpart[3]);
                stcklist.Add(stck);
                }

            }

           Log.Information("El tiempo tomado en importar el CSV en memoria es:  " + (stopwatchgral.ElapsedMilliseconds / 1000) / 60 + " minutos");


            var copyParameters = new[]
             {
                        nameof(SaleStock.PointOfSale),
                        nameof(SaleStock.Product),
                        nameof(SaleStock.Date),  
                        nameof(SaleStock.Stock)

                    };

            Log.Information("Comenzando a copiar el archivo a la BD");
            stopwatchgral.Reset();
            stopwatchgral.Start();

            using (var sqlCopy = new SqlBulkCopy(Configuration.GetConnectionString("BloggingDatabase")))
            {
                sqlCopy.DestinationTableName = "[Stock]";
                sqlCopy.BatchSize = 500;
                using (var reader = ObjectReader.Create(stcklist, copyParameters))
                {
                    sqlCopy.WriteToServer(reader);
                }
            }


            Log.Information("Fin de copiado del archivo a la BD!");          
            Log.Information("El tiempo tomado en importar el CSV en seg es:  " + (stopwatchgral.ElapsedMilliseconds / 1000) + " segundos");
            Log.Information("El tiempo tomado en importar el CSV en min es:  " + (stopwatchgral.ElapsedMilliseconds / 1000) / 60 + " minutos");
            stopwatchgral.Stop();
        }


        private static void  crearArchivoLogeo() {

            var dir = Directory.CreateDirectory("Log");
            var fullpath = dir.FullName + "\\" + "Log_" + DateTime.Now.ToString("yyyy_dd_M_HH_mm_ss") + ".txt";

            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .WriteTo.Console()
                .WriteTo.File(fullpath)
            .CreateLogger();

        }


        public static SingleTransferContext GetSingleTransferContext(TransferCheckpoint checkpoint, Stopwatch stopwatchTarea)
        {
            SingleTransferContext context = new SingleTransferContext(checkpoint);

            context.ProgressHandler = new Progress<TransferStatus>((progress) =>
            {
                Log.Information("Mb descargados {0:F}\n", (progress.BytesTransferred / 1024) / 1024);
                stopwatchTarea.Restart();
            });

            return context;
        }



        static void retryTransferencia(SingleTransferContext context, CloudBlockBlob cloudBlockBlob, string localFile) {
            
            CancellationTokenSource cancellationSource = new CancellationTokenSource();
            Stopwatch stopwatchTareaRetry = new Stopwatch();
            stopwatchTareaRetry.Start();


            TransferCheckpoint checkpoint = context.LastCheckpoint;
            context = GetSingleTransferContext(checkpoint, stopwatchTareaRetry);

            Console.WriteLine("\nReanudando la transferencia...\n");


            try
            {
                DownloadOptions optionsWithDisableContentMD5Validation2 = new DownloadOptions() { DisableContentMD5Validation = true };

                var taskRetry = TransferManager.DownloadAsync(cloudBlockBlob, localFile, optionsWithDisableContentMD5Validation2, context, cancellationSource.Token);

                evaluacionDeCaracteresDeErrorYretry(taskRetry, stopwatchTareaRetry);

                taskRetry.Wait();
            }
            catch (Exception e)
            {
                Log.Error("\nLa transferencia fue cancelada : {0}", e.Message);
                cancellationSource.Cancel();

            }

            if (cancellationSource.IsCancellationRequested)
            {
                Console.WriteLine("\nLa transferencia será reanudada en 3 segundos...");
                Thread.Sleep(3000);

                retryTransferencia(context, cloudBlockBlob, localFile);

            }


           





        }


        static void evaluacionDeCaracteresDeErrorYretry(Task task, Stopwatch tiempoRetry) {
            while (!task.IsCompleted)
            {
                if (Console.KeyAvailable)
                {
                    ConsoleKeyInfo keyinfo = Console.ReadKey(true);
                    if (keyinfo.Key == ConsoleKey.C)
                    {
                        throw new Exception("Oh no ! Se ha producido un error de cancelacion intencional !!!");                       
                    }

                    if (keyinfo.Key == ConsoleKey.E)
                    {
                        throw new TimeoutException("Oh no ! Se ha producido un error de time out intencional !!!");
                    }
                } else if (tiempoRetry.ElapsedMilliseconds/1000>4*60 ) {

                    throw new TimeoutException("Haciendo retry porque han pasado: "+ tiempoRetry.ElapsedMilliseconds / 1000 + " segundos sin transferencia" );

                }


            }
        }




       public static void ejecutarScriptBD(){

                SqlConnection conn = new SqlConnection(Configuration.GetConnectionString("BloggingDatabase"));
                try
                {

                    conn.Open();

                    string script = File.ReadAllText("../../../Script/SQL_Script_1.sql");

                    // split script on GO command
                    IEnumerable<string> commandStrings = Regex.Split(script, @"^\s*GO\s*$", RegexOptions.Multiline | RegexOptions.IgnoreCase);
                    foreach (string commandString in commandStrings)
                    {
                        if (commandString.Trim() != "")
                        {
                            new SqlCommand(commandString, conn).ExecuteNonQuery();
                        }
                    }
                Log.Information("Script ejecutado correctamente en la BD ! ");

            }
                catch (SqlException er)
                {
                Log.Error("Error en la insercion de datos a la BD :" + er);
            }
                finally
                {
                    conn.Close();
                }
            }




        



    }
}
