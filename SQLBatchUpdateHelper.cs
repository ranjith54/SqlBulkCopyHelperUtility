using System.ComponentModel.DataAnnotations.Schema;
using System.Data.SqlClient;
using System.Text;

namespace SqlBulkCopyHelperUtility
{
    public class SQLBatchUpdateHelper
    {
        /// <summary>
        /// Executes a batch update of items in the specified database table.
        /// The method processes items in batches, constructs an SQL UPDATE statement for each batch,
        /// and executes the update within the provided transaction.
        /// </summary>
        /// <typeparam name="T">The type of the items to be updated.</typeparam>
        /// <param name="conn">The SQL connection to use for the update.</param>
        /// <param name="sqlTransaction">The SQL transaction to use for the update.</param>
        /// <param name="items">The list of items to update.</param>
        /// <param name="keyProperties">The properties that identify each item uniquely (i.e., the primary key properties).</param>
        /// <param name="propertiesToUpdate">The properties that should be updated in each item.</param>
        /// <exception cref="ArgumentException">Thrown if the items list is null or empty, or if the table name is invalid.</exception>
        public static void BatchUpdate<T>(
            SqlConnection conn,
            SqlTransaction sqlTransaction,
            List<T> items,
            string[] keyProperties,
            string[] propertiesToUpdate)
        {
            // Validate inputs
            if (items == null || !items.Any())
                throw new ArgumentException("The items list cannot be null or empty.", nameof(items));

            if (keyProperties == null || !keyProperties.Any() || propertiesToUpdate == null || !propertiesToUpdate.Any())
                throw new ArgumentException("The keyProperties or PropertiesToUpdate cannot be null or empty.");

            StringBuilder updateString = new StringBuilder();
            int totalItems = items.Count;
            int batchSize = 300;

            // Get the ColumnAttribute applied to the property
            var tableAttribute = typeof(T).GetCustomAttributes(typeof(TableAttribute), false)
                                          .FirstOrDefault() as TableAttribute;

            string table = tableAttribute?.Name;

            if (string.IsNullOrWhiteSpace(table))
                throw new ArgumentException("Table name cannot be null or empty.", nameof(table));

            for (int i = 0; i < totalItems; i += batchSize)
            {
                int currentBatchSize = Math.Min(batchSize, totalItems - i);
                updateString.Clear();
                updateString.Append($"UPDATE {table} SET ");

                // Build SET part for properties to update
                foreach (string prop in propertiesToUpdate)
                {
                    updateString.Append($"{prop} = x.{prop}, ");
                }

                updateString.Length -= 2; // Remove the last comma

                updateString.Append(" FROM " + table + " INNER JOIN (VALUES ");

                var parameters = new List<SqlParameter>();

                // Append parameters for the current batch
                for (int j = 0; j < currentBatchSize; j++)
                {
                    var currentItem = items[i + j];
                    parameters.AddRange(CreateParameters(currentItem, propertiesToUpdate, i + j));
                    parameters.AddRange(CreateParameters(currentItem, keyProperties, i + j));

                    updateString.Append($"({string.Join(", ", keyProperties.Select(x => $"@{x}_{i + j}"))}, {string.Join(", ", propertiesToUpdate.Select(p => $"@{p}_{i + j}"))}), ");
                }

                updateString.Length -= 2; // Remove the last comma and space
                updateString.Append($") AS x({string.Join(", ", keyProperties)}, {string.Join(", ", propertiesToUpdate)}) ON ");

                // Build the ON condition using key properties
                foreach (var prop in keyProperties)
                {
                    updateString.Append($"x.{prop} = {table}.{prop} AND ");
                }
                updateString.Length -= 5; // Remove the last AND

                // Execute the batch update
                using (SqlCommand updateCmd = new SqlCommand(updateString.ToString(), conn, sqlTransaction))
                {
                    updateCmd.Parameters.AddRange(parameters.ToArray());
                    updateCmd.ExecuteNonQuery();
                }
            }
        }

        /// <summary>
        /// Creates SQL parameters from the properties of the specified item.
        /// Each parameter is uniquely named by appending the index to the property name.
        /// </summary>
        /// <typeparam name="T">The type of the item.</typeparam>
        /// <param name="currentItem">The item from which to create parameters.</param>
        /// <param name="properties">The properties to create parameters for (based on column names).</param>
        /// <param name="index">The index used to create unique parameter names.</param>
        /// <returns>A collection of SQL parameters created from the item's properties.</returns>
        /// <exception cref="ArgumentException">Thrown if a matching property for a column is not found.</exception>
        private static IEnumerable<SqlParameter> CreateParameters<T>(T currentItem, string[] properties, int index)
        {
            var parameters = new List<SqlParameter>();
            foreach (var prop in properties)
            {
                var propertyName = GetPropertyNameByColumnName<T>(prop) ?? throw new ArgumentException($"No property found for column '{prop}'");
                var value = typeof(T).GetProperty(propertyName)?.GetValue(currentItem);
                parameters.Add(new SqlParameter($"@{prop}_{index}", value ?? DBNull.Value));
            }
            return parameters;
        }

        /// <summary>
        /// Retrieves the property name that corresponds to a given column name, based on the ColumnAttribute applied to the property.
        /// Uses a cache to improve performance for subsequent lookups.
        /// </summary>
        /// <typeparam name="T">The type of the class containing the properties.</typeparam>
        /// <param name="columnName">The name of the column in the database.</param>
        /// <returns>The name of the property that corresponds to the column name, or null if no match is found.</returns>
        private static string GetPropertyNameByColumnName<T>(string columnName)
        {
            // Check if the result is already cached
            string cacheKey = $"{typeof(T).FullName}.{columnName}";
            if (PropertyNameCache.TryGetValue(cacheKey, out var cachedPropertyName))
            {
                return cachedPropertyName;
            }

            // Get all properties of the class
            var properties = typeof(T).GetProperties();

            foreach (var property in properties)
            {
                // Get the Column attribute, if present
                var columnAttr = property.GetCustomAttributes(typeof(ColumnAttribute), false)
                                         .FirstOrDefault() as ColumnAttribute;

                if (columnAttr != null && columnAttr.Name == columnName)
                {
                    // Cache the result for future use
                    PropertyNameCache[cacheKey] = property.Name;
                    return property.Name;
                }
            }

            // If no match is found, return null
            return null;
        }

        /// <summary>
        /// A dictionary that stores mappings between column names and property names for caching.
        /// This improves performance by avoiding repeated reflection calls.
        /// </summary>
        private static readonly Dictionary<string, string> PropertyNameCache = new Dictionary<string, string>();
    }
}
