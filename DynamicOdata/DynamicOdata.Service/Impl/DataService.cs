using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Linq;
using System.Web.Http.OData;
using System.Web.Http.OData.Query;
using Dapper;
using Microsoft.Data.Edm;
using Microsoft.Data.Edm.Library;

namespace DynamicOdata.Service.Impl
{
    public class DataService : IDataService
    {
        private readonly string _connectionString;

        public DataService(string clientName)
        {
            _connectionString = ConfigurationManager.ConnectionStrings[clientName].ConnectionString;
        }

        private EdmEntityObject CreateEdmEntity(IEdmEntityType entityType, dynamic row)
        {
            if (row == null)
                return null;

            var entity = new EdmEntityObject(entityType);
            IDictionary<string, object> propertyMap = row as IDictionary<string, object>;

            if (propertyMap != null)
            {
                foreach (var propertyPair in propertyMap)
                    entity.TrySetPropertyValue(propertyPair.Key, propertyPair.Value);
            }

            return entity;
        }

        public int Count(IEdmCollectionType collectionType, ODataQueryOptions queryOptions)
        {
            var entityType = collectionType.ElementType.Definition as EdmEntityType;
            int count = 0;

            if (entityType != null)
            {
                using (var connection = new SqlConnection(_connectionString))
                {
                    var sqlBuilder = new SqlQueryBuilder(queryOptions);
                    count = connection.Query<int>(sqlBuilder.ToCountSql()).Single();
                }
            }

            return count;
        }
        //http://localhost:7992/odata/gotocard_dev/User?$filter=Email%20eq%20%27mathew.newport@metc.state.mn.us%27

        public EdmEntityObjectCollection Get(IEdmCollectionType collectionType, ODataQueryOptions queryOptions)
        {
            var entityType = collectionType.ElementType.Definition as EdmEntityType;
            var collection = new EdmEntityObjectCollection(new EdmCollectionTypeReference(collectionType, true));

            if (entityType != null)
            {
                using (var connection = new SqlConnection(_connectionString))
                {
                    var sqlBuilder = new SqlQueryBuilder(queryOptions);
                    var sql = sqlBuilder.ToSql();
                    IEnumerable<dynamic> rows = connection.Query<dynamic>(sql);

                    foreach (dynamic row in rows)
                    {
                        var entity = CreateEdmEntity(entityType, row);
                        collection.Add(entity);
                    }
                }
            }

            return collection;
        }

        public EdmEntityObject Get(string key, IEdmEntityType entityType)
        {
            List<IEdmStructuralProperty> keys = null;
            if (entityType.DeclaredKey != null && entityType.DeclaredKey.Any())
                 keys = entityType.DeclaredKey.ToList();
            else
            {
                //grab the first column, just assume it's a key...
                var firstItem = entityType.DeclaredProperties.First();
                keys = new List<IEdmStructuralProperty>();
                keys.Add(new EdmStructuralProperty(
                    firstItem.DeclaringType,
                    firstItem.Name,
                    firstItem.Type));
            }

            // make sure entity type has unique key, not composite key
            if (keys.Count != 1)
                return null;

            var dbArgs = new DynamicParameters();
            dbArgs.Add(keys.First().Name, key);

            var sql = $@"SELECT top(1) * FROM [{entityType.Namespace}].[{entityType.Name}] WHERE [{keys.First().Name}] = @{keys.First().Name}";

            using (var connection = new SqlConnection(_connectionString))
            {
                var row = connection.QueryFirst(sql, dbArgs);

                var entity = CreateEdmEntity(entityType, row);
                return entity;
            }
        }
    }
}