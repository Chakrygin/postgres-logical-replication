using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using Npgsql;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput;

namespace PostgresLogicalReplicationApp.Timeouts
{
    public static class Program
    {
        public static async Task Main(string[] args)
        {
            var csb = new NpgsqlConnectionStringBuilder
            {
                Host = "127.0.0.1",
                Port = 5432,
                Database = "postgres",
                Username = "postgres",
                Password = "postgres",
            };

            await using (var connection = new NpgsqlConnection(csb.ToString()))
            {
                // Cleanup...
                
                await connection.ExecuteAsync(
                    $"DROP SCHEMA public CASCADE");
                
                await connection.ExecuteAsync(
                    $"CREATE SCHEMA public");
                
                await connection.ExecuteAsync(
                    $"DROP PUBLICATION IF EXISTS test_pub");
                
                await connection.ExecuteAsync(
                    $"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'test_slot';");
                
                // Setup...
                
                await connection.ExecuteAsync(
                    $"CREATE TABLE test(id INT PRIMARY KEY, text TEXT NOT NULL)");
                
                await connection.ExecuteAsync(
                    $"CREATE PUBLICATION test_pub FOR TABLE test");
                
                await connection.ExecuteAsync(
                    $"SELECT * FROM pg_create_logical_replication_slot('test_slot', 'pgoutput')");
            }

            await using (var connection = new LogicalReplicationConnection(csb.ToString()))
            {
                await connection.Open();
                
                var slot = new PgOutputReplicationSlot("test_slot");
                var options = new PgOutputReplicationOptions("test_pub");
                
                while (true)
                {
                    var sw = Stopwatch.StartNew();
                    
                    var cancellationTokenSource = new CancellationTokenSource();
                    var cancellationToken = cancellationTokenSource.Token;
                    
                    try
                    {
                        await foreach (var message in connection.StartReplication(slot, options, cancellationToken))
                        {
                        }
                    }
                    catch (NpgsqlException ex)
                    {
                        Console.WriteLine("ELAPSED: " + sw.Elapsed);
                        Console.WriteLine(ex);
                        Console.WriteLine();
                    }
                }
            }
        }
    }
}