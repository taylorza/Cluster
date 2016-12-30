using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cluster
{
    class Program
    {
        static void Main(string[] args)
        {
            ClusterServer server = new ClusterServer(int.Parse(args[0]));
            Task t = server.Start();
            Console.WriteLine("Server Started...");
            int x = Console.CursorLeft;
            int y = Console.CursorTop;
            var defaultForeground = Console.ForegroundColor;
            while(!Console.KeyAvailable)
            {
                var activeNodes = server.GetActiveNodes();
                Console.SetCursorPosition(x, y);
                Console.ForegroundColor = ConsoleColor.Green;
                Console.Write($"Active Nodes : {activeNodes.Count:000}\t\t");
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"Dead Nodes : {server.GetDeadNodes().Count:000}");
                Console.WriteLine();
                Console.Write(new string(' ', 80));
                Console.Write("\r");
                foreach(var node in activeNodes)
                {
                    if (node.ErrorCount == 0) Console.ForegroundColor = ConsoleColor.Green;
                    else if (node.ErrorCount == 1) Console.ForegroundColor = ConsoleColor.Yellow;
                    else if (node.ErrorCount == 2) Console.ForegroundColor = ConsoleColor.DarkYellow;
                    else Console.ForegroundColor = ConsoleColor.Red;
                    if (node.ErrorCount == 0)
                        Console.Write("O ");
                    else
                        Console.Write("o ");
                }
                
                Console.WriteLine();
                Console.WriteLine();
                Console.ForegroundColor = defaultForeground;
                Console.WriteLine("Press any key to exit");
                Thread.Sleep(1000);
                SetThreadExecutionState(EXECUTION_STATE.ES_CONTINUOUS | EXECUTION_STATE.ES_SYSTEM_REQUIRED);  

            }
            server.Shutdown();
        }

        [DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true)]
        static extern EXECUTION_STATE SetThreadExecutionState(EXECUTION_STATE esFlags);

        [FlagsAttribute]
        public enum EXECUTION_STATE : uint
        {
            ES_AWAYMODE_REQUIRED = 0x00000040,
            ES_CONTINUOUS = 0x80000000,
            ES_DISPLAY_REQUIRED = 0x00000002,
            ES_SYSTEM_REQUIRED = 0x00000001
        }

    }
}
