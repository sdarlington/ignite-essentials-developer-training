using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Threading.Tasks;
using Apache.Ignite;
using Apache.Ignite.Compute;
using Apache.Ignite.Sql;
using Apache.Ignite.Table;
using Apache.Ignite.Transactions;

namespace Training
{
    public class ComputeApp
    {
        private static readonly DeploymentUnit DeploymentUnit =
            new DeploymentUnit("essentialsCompute", "1.0.0");

        public static async Task Main(string[] args)
        {
            await using var ignite = await IgniteClient.StartAsync(new IgniteClientConfiguration
            {
                Endpoints = new[] { "127.0.0.1:10800" }
            });

            await CalculateTopPayingCustomers(ignite);

            await Task.Delay(5000);
        }

        private static async Task CalculateTopPayingCustomers(IIgniteClient ignite)
        {
            int customersCount = 5;

            var job = TaskDescriptor
                .Builder(typeof(TopPayingCustomersTask))
                .Units(DeploymentUnit)
                .Build();

            var results = await ignite.Compute.ExecuteMapReduceAsync<int, TopCustomer[]>(job, customersCount);

            PrintTopPayingCustomers(results.ToList(), customersCount);
        }

        /// <summary>
        /// Task executed on every cluster node that calculates top local paying customers.
        /// </summary>
        public class TopPayingCustomersTask :
            IMapReduceTask<int, Tuple, CustomerPrice[], TopCustomer[]>
        {
            private int _customerCount;

            public async Task<List<IMapReduceJob<Tuple, CustomerPrice[]>>> SplitAsync(
                ITaskExecutionContext context,
                int customersCount)
            {
                _customerCount = customersCount;

                var table = await context.Ignite.Tables.GetTableAsync("InvoiceLine");
                var replicas = await table.PartitionManager.GetPrimaryReplicasAsync();

                return replicas.Select(replica =>
                        MapReduceJob
                            .Builder<Tuple, CustomerPrice[]>()
                            .Nodes(new[] { replica.Value })
                            .Args(Tuple.Create()
                                .Set("partition", replica.Key.GetHashCode())
                                .Set("count", customersCount))
                            .JobDescriptor(
                                JobDescriptor
                                    .Builder(typeof(TopPayingCustomersJob))
                                    .Units(DeploymentUnit)
                                    .Build())
                            .Build())
                    .ToList<IMapReduceJob<Tuple, CustomerPrice[]>>();
            }

            public async Task<TopCustomer[]> ReduceAsync(
                ITaskExecutionContext context,
                IDictionary<Guid, CustomerPrice[]> map)
            {
                var orderedResults = new List<CustomerPrice>();

                foreach (var result in map.Values)
                {
                    orderedResults.AddRange(result.Where(x => x != null));
                }

                orderedResults = orderedResults
                    .OrderByDescending(x => x.Price)
                    .ToList();

                var customersTable = await context.Ignite.Tables.GetTableAsync("Customer");
                var customersCache = customersTable.GetRecordView<Tuple>();

                var results = new List<TopCustomer>();

                for (int i = 0; i < orderedResults.Count && i < _customerCount; i++)
                {
                    var key = orderedResults[i].CustomerId;

                    var customerRecord = await customersCache.GetAsync(
                        null,
                        Tuple.Create().Set("customerId", key));

                    var customer = new TopCustomer(key, orderedResults[i].Price);

                    if (customerRecord != null)
                    {
                        customer.FullName =
                            $"{customerRecord.Get<string>("firstName")} {customerRecord.Get<string>("lastName")}";

                        customer.City = customerRecord.Get<string>("city");
                        customer.Country = customerRecord.Get<string>("country");
                    }
                    else
                    {
                        customer.FullName = "unknown";
                        customer.City = "unknown";
                        customer.Country = "unknown";
                    }

                    results.Add(customer);
                }

                return results.ToArray();
            }

            /// <summary>
            /// Compute job executed on each node.
            /// </summary>
            public class TopPayingCustomersJob : IComputeJob<Tuple, CustomerPrice[]>
            {
                private const string Sql =
                    "select customerid, quantity * unitprice as price from invoiceline where \"__part\" = ?";

                public async Task<CustomerPrice[]> ExecuteAsync(
                    IJobExecutionContext context,
                    Tuple parameters)
                {
                    var customerPurchases = new Dictionary<int, decimal>();

                    int customerCount = parameters.Get<int>("count");
                    int partition = parameters.Get<int>("partition");

                    await using var results = await context.Ignite.Sql.ExecuteAsync(
                        null,
                        Sql,
                        partition);

                    await foreach (var row in results)
                    {
                        int customerId = row.Get<int>("customerId");
                        decimal price = row.Get<decimal>("price");

                        if (customerPurchases.ContainsKey(customerId))
                            customerPurchases[customerId] += price;
                        else
                            customerPurchases[customerId] = price;
                    }

                    var ordered = customerPurchases
                        .OrderByDescending(x => x.Value)
                        .Take(customerCount)
                        .Select(x => new CustomerPrice(x.Key, x.Value))
                        .ToArray();

                    return ordered;
                }
            }
        }

        private static void PrintTopPayingCustomers(List<TopCustomer> results, int count)
        {
            Console.WriteLine($">>> Top {count} Paying Listeners Across All Cluster Nodes");

            foreach (var customer in results)
            {
                Console.WriteLine(customer);
            }
        }
    }

    public class CustomerPrice
    {
        public int CustomerId { get; }
        public decimal Price { get; }

        public CustomerPrice(int customerId, decimal price)
        {
            CustomerId = customerId;
            Price = price;
        }
    }

    public class TopCustomer
    {
        public int CustomerId { get; }
        public decimal Price { get; }

        public string FullName { get; set; }
        public string City { get; set; }
        public string Country { get; set; }

        public TopCustomer(int customerId, decimal price)
        {
            CustomerId = customerId;
            Price = price;
        }

        public override string ToString()
        {
            return $"{FullName} ({City}, {Country}) - {Price}";
        }
    }
}
