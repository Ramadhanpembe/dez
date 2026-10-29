from pyflink.table import EnvironmentSettings, TableEnvironment

env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())

# 1. Simple table from in-memory data
t = env.from_elements(
    [(1, "zone_A", 10.5), (2, "zone_A", 20.0), (3, "zone_B", 7.25)],
    ["id", "zone", "amount"],
)
print("--- raw rows ---")
t.execute().print()

# 2. A SQL aggregation, like the rides-per-zone idea
env.create_temporary_view("rides", t)
print("--- revenue per zone ---")
env.execute_sql(
    "SELECT zone, COUNT(*) AS num_rides, SUM(amount) AS revenue "
    "FROM rides GROUP BY zone"
).print()