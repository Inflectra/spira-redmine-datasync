using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Inflectra.SpiraTest.AddOns.RedmineDataSync.SpiraImportExport;

namespace Inflectra.SpiraTest.AddOns.RedmineDataSync
{
    /// <summary>
    /// Contains helper-functions used by the data-sync
    /// </summary>
    public static class InternalFunctions
    {
        /// <summary>
        /// Parse a string to an integer, throw an exception if not
        /// </summary>
        /// <param name="str"></param>
        /// <returns></returns>
        public static int ToSafeInt32(string str, string fieldName = "")
        {
            if (str == null)
            {
                throw new InvalidOperationException("Unable to convert Null value to integer, please make sure field " + fieldName + " is numeric");
            }

            int intValue;
            if (Int32.TryParse(str, out intValue))
            {
                return intValue;
            }

            throw new InvalidOperationException("Unable to convert value '" + str + "' to integer, please make sure field " + fieldName + " is numeric");
        }

        /// <summary>
        /// Finds a mapping entry from the internal id and project id
        /// </summary>
        /// <param name="projectId">The project id</param>
        /// <param name="internalId">The internal id</param>
        /// <param name="dataMappings">The list of mappings</param>
        /// <returns>The matching entry or Null if none found</returns>
        public static SpiraImportExport.RemoteDataMapping FindMappingByInternalId(int projectId, int internalId, SpiraImportExport.RemoteDataMapping[] dataMappings)
        {
            foreach (SpiraImportExport.RemoteDataMapping dataMapping in dataMappings)
            {
                if (dataMapping.InternalId == internalId && dataMapping.ProjectId == projectId)
                {
                    return dataMapping;
                }
            }
            return null;
        }

        /// <summary>
        /// Finds a mapping entry from the external key and project id
        /// </summary>
        /// <param name="projectId">The project id</param>
        /// <param name="externalKey">The external key</param>
        /// <param name="dataMappings">The list of mappings</param>
        /// <param name="onlyPrimaryEntries">Do we only want to locate primary entries</param>
        /// <returns>The matching entry or Null if none found</returns>
        public static SpiraImportExport.RemoteDataMapping FindMappingByExternalKey(int projectId, string externalKey, SpiraImportExport.RemoteDataMapping[] dataMappings, bool onlyPrimaryEntries)
        {
            foreach (SpiraImportExport.RemoteDataMapping dataMapping in dataMappings)
            {
                if (dataMapping.ExternalKey == externalKey && dataMapping.ProjectId == projectId)
                {
                    //See if we're only meant to return primary entries
                    if (!onlyPrimaryEntries || dataMapping.Primary)
                    {
                        return dataMapping;
                    }
                }
            }
            return null;
        }

        /// <summary>
        /// Finds a mapping entry from the internal id
        /// </summary>
        /// <param name="internalId">The internal id</param>
        /// <param name="dataMappings">The list of mappings</param>
        /// <returns>The matching entry or Null if none found</returns>
        /// <remarks>Used when no project id stored in the mapping collection</remarks>
        public static SpiraImportExport.RemoteDataMapping FindMappingByInternalId(int internalId, SpiraImportExport.RemoteDataMapping[] dataMappings)
        {
            foreach (SpiraImportExport.RemoteDataMapping dataMapping in dataMappings)
            {
                if (dataMapping.InternalId == internalId)
                {
                    return dataMapping;
                }
            }
            return null;
        }

        /// <summary>
        /// Finds a mapping entry from the external key
        /// </summary>
        /// <param name="externalKey">The external key</param>
        /// <param name="dataMappings">The list of mappings</param>
        /// <returns>The matching entry or Null if none found</returns>
        /// <remarks>Used when no project id stored in the mapping collection</remarks>
        public static SpiraImportExport.RemoteDataMapping FindMappingByExternalKey(string externalKey, SpiraImportExport.RemoteDataMapping[] dataMappings)
        {
            foreach (SpiraImportExport.RemoteDataMapping dataMapping in dataMappings)
            {
                if (dataMapping.ExternalKey == externalKey)
                {
                    return dataMapping;
                }
            }
            return null;
        }

        /// <summary>
        /// Sets a custom property value on an artifact, even if it doesn't have an entry yet, can handle the various types
        /// </summary>
        /// <param name="remoteArtifact">The artifact we're setting the properties on</param>
        /// <param name="propertyNumber">The position number (1-30) of the custom property</param>
        /// <param name="propertyValue">The typed property value</param>
        public static void SetCustomPropertyValue<T>(RemoteArtifact remoteArtifact, int propertyNumber, T propertyValue)
        {
            //First see if we have any custom properties at all for this artifact, if not, create a collection
            List<RemoteArtifactCustomProperty> artifactCustomProperties;
            if (remoteArtifact.CustomProperties == null)
            {
                artifactCustomProperties = new List<RemoteArtifactCustomProperty>();
            }
            else
            {
                artifactCustomProperties = remoteArtifact.CustomProperties.ToList();
            }

            //Now see if we have a matching property already in the list
            RemoteArtifactCustomProperty artifactCustomProperty = artifactCustomProperties.FirstOrDefault(c => c.PropertyNumber == propertyNumber);
            if (artifactCustomProperty == null)
            {
                artifactCustomProperty = new RemoteArtifactCustomProperty();
                artifactCustomProperty.PropertyNumber = propertyNumber;
                artifactCustomProperties.Add(artifactCustomProperty);
            }

            //Set the value that matches this type
            if (typeof(T) == typeof(String))
            {
                artifactCustomProperty.StringValue = ((T)propertyValue as String);
            }
            if (typeof(T) == typeof(Int32) || typeof(T) == typeof(Nullable<Int32>))
            {
                artifactCustomProperty.IntegerValue = ((T)propertyValue as Int32?);
            }

            if (typeof(T) == typeof(Boolean) || typeof(T) == typeof(Nullable<Boolean>))
            {
                artifactCustomProperty.BooleanValue = ((T)propertyValue as bool?);
            }

            if (typeof(T) == typeof(DateTime) || typeof(T) == typeof(Nullable<DateTime>))
            {
                artifactCustomProperty.DateTimeValue = ((T)propertyValue as DateTime?);
            }

            if (typeof(T) == typeof(Decimal) || typeof(T) == typeof(Nullable<Decimal>))
            {
                artifactCustomProperty.DecimalValue = ((T)propertyValue as Decimal?);
            }

            if (typeof(T) == typeof(Int32[]))
            {
                artifactCustomProperty.IntegerListValue = ((T)propertyValue as Int32[]);
            }

            if (typeof(T) == typeof(List<Int32>))
            {
                List<Int32> intList = (List<Int32>)((T)propertyValue as List<Int32>);
                if (intList == null || intList.Count == 0)
                {
                    artifactCustomProperty.IntegerListValue = null;
                }
                else
                {
                    artifactCustomProperty.IntegerListValue = intList.ToArray();
                }
            }

            //Finally we need to update the artifact's array
            remoteArtifact.CustomProperties = artifactCustomProperties.ToArray();
        }

        /// <summary>
        /// Gets the deserialized custom property value in a format that can be handled by TFS
        /// </summary>
        /// <param name="artifactCustomProperty">The artifact custom property</param>
        /// <returns></returns>
        /// <remarks>Not to be used for multi-valued list properties</remarks>
        public static object GetCustomPropertyValue(RemoteArtifactCustomProperty artifactCustomProperty)
        {
            //See if we have value on one of the non-string types
            if (artifactCustomProperty.BooleanValue.HasValue)
            {
                return artifactCustomProperty.BooleanValue.Value;
            }
            if (artifactCustomProperty.DateTimeValue.HasValue)
            {
                return artifactCustomProperty.DateTimeValue.Value;
            }
            if (artifactCustomProperty.DecimalValue.HasValue)
            {
                return artifactCustomProperty.DecimalValue.Value;
            }
            if (artifactCustomProperty.IntegerValue.HasValue)
            {
                return artifactCustomProperty.IntegerValue.Value;
            }

            //Otherwise just return the string value
            return artifactCustomProperty.StringValue;
        }

        /// <summary>
        /// Renders HTML content as plain text, since JIRA cannot handle tags
        /// </summary>
        /// <param name="source">The HTML markup</param>
        /// <returns>Plain text representation</returns>
        /// <remarks>Handles line-breaks, etc.</remarks>
        public static string HtmlRenderAsPlainText(string source)
        {
            try
            {
                string result;

                // Remove HTML Development formatting
                // Replace line breaks with space
                // because browsers inserts space
                result = source.Replace("\r", " ");
                // Replace line breaks with space
                // because browsers inserts space
                result = result.Replace("\n", " ");
                // Remove step-formatting
                result = result.Replace("\t", string.Empty);
                // Remove repeating speces becuase browsers ignore them
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"( )+", " ");

                // Remove the header (prepare first by clearing attributes)
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"<( )*head([^>])*>", "<head>",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"(<( )*(/)( )*head( )*>)", "</head>",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    "(<head>).*(</head>)", string.Empty,
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);

                // remove all scripts (prepare first by clearing attributes)
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"<( )*script([^>])*>", "<script>",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"(<( )*(/)( )*script( )*>)", "</script>",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                //result = System.Text.RegularExpressions.Regex.Replace(result, 
                //         @"(<script>)([^(<script>\.</script>)])*(</script>)",
                //         string.Empty, 
                //         System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"(<script>).*(</script>)", string.Empty,
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);

                // remove all styles (prepare first by clearing attributes)
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"<( )*style([^>])*>", "<style>",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"(<( )*(/)( )*style( )*>)", "</style>",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    "(<style>).*(</style>)", string.Empty,
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);

                // insert tabs in spaces of <td> tags
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"<( )*td([^>])*>", "\t",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);

                // insert line breaks in places of <BR> and <LI> tags
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"<( )*br( )*>", "\r",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"<( )*br( )*/>", "\r",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"<( )*li( )*>", "\r",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);

                // insert line paragraphs (double line breaks) in place
                // if <P>, <DIV> and <TR> tags
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"<( )*div([^>])*>", "\r\r",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"<( )*tr([^>])*>", "\r\r",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"<( )*p([^>])*>", "\r\r",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);

                // Remove remaining tags like <a>, links, images,
                // comments etc - anything thats enclosed inside < >
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"<[^>]*>", string.Empty,
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);

                // replace special characters:
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"&nbsp;", " ",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);

                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"&bull;", " * ",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"&lsaquo;", "<",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"&rsaquo;", ">",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"&trade;", "(tm)",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"&frasl;", "/",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"<", "<",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @">", ">",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"&copy;", "(c)",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"&reg;", "(r)",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                // Remove all others. More can be added, see
                // http://hotwired.lycos.com/webmonkey/reference/special_characters/
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    @"&(.{2,6});", string.Empty,
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);

                // for testng
                //System.Text.RegularExpressions.Regex.Replace(result, 
                //       this.txtRegex.Text,string.Empty, 
                //       System.Text.RegularExpressions.RegexOptions.IgnoreCase);

                // make line breaking consistent
                result = result.Replace("\n", "\r");

                // Remove extra line breaks and tabs:
                // replace over 2 breaks with 2 and over 4 tabs with 4. 
                // Prepare first to remove any whitespaces inbetween
                // the escaped characters and remove redundant tabs inbetween linebreaks
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    "(\r)( )+(\r)", "\r\r",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    "(\t)( )+(\t)", "\t\t",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    "(\t)( )+(\r)", "\t\r",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    "(\r)( )+(\t)", "\r\t",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                // Remove redundant tabs
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    "(\r)(\t)+(\r)", "\r\r",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                // Remove multible tabs followind a linebreak with just one tab
                result = System.Text.RegularExpressions.Regex.Replace(result,
                    "(\r)(\t)+", "\r\t",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                // Initial replacement target string for linebreaks
                string breaks = "\r\r\r";
                // Initial replacement target string for tabs
                string tabs = "\t\t\t\t\t";
                for (int index = 0; index < result.Length; index++)
                {
                    result = result.Replace(breaks, "\r\r");
                    result = result.Replace(tabs, "\t\t\t\t");
                    breaks = breaks + "\r";
                    tabs = tabs + "\t";
                }

                // Thats it.
                return result;

            }
            catch
            {
                return source;
            }
        }
    }
}
