using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.Globalization;
using System.ServiceModel;

using Inflectra.SpiraTest.PlugIns;
using Inflectra.SpiraTest.AddOns.RedmineDataSync.SpiraImportExport;
using Redmine.Net.Api;
using Redmine.Net.Api.Types;
using System.Collections.Specialized;
using System.Net;

namespace Inflectra.SpiraTest.AddOns.RedmineDataSync
{
    /// <summary>
    /// Sample data-synchronization provider that synchronizes incidents between SpiraTest/Plan/Team and Redmine
    /// </summary>
    /// <remarks>
    /// Requires Spira v4.0 or newer since it uses the v4.0+ compatible web service API
    /// </remarks>
    public class DataSync : IDataSyncPlugIn
    {
        //Constant containing data-sync name and internal API URL suffix to access
        private const string DATA_SYNC_NAME = "RedmineDataSync"; //The name of the data-synchronization plugin
        private const string EXTERNAL_SYSTEM_NAME = "Redmine";  //The name of the external system we're integrating with
        private const string EXTERNAL_BUG_URL = "/issues/{0}";  //The URL format to use to link incidents to (leave null to not add a link)

        // Track whether Dispose has been called.
        private bool disposed = false;

        //Configuration data passed through from calling service
        private EventLog eventLog;
        private bool traceLogging;
        private int dataSyncSystemId;
        private string webServiceBaseUrl;
        private string internalLogin;
        private string internalPassword;
        private string connectionString;
        private string externalLogin;
        private string externalPassword;
        private int timeOffsetHours;
        private bool autoMapUsers;

        //Custom configuration properties
        private bool createNewItemsInSpira;
        private bool createNewItemsInRedmine;
        private string custom03;
        private string custom04;
        private string custom05;

        /// <summary>
        /// Constructor, does nothing - all setup in the Setup() method instead
        /// </summary>
        public DataSync()
        {
            //Does Nothing - all setup in the Setup() method instead
        }

        /// <summary>
        /// Loads in all the configuration information passed from the calling service
        /// </summary>
        /// <param name="eventLog">Handle to the event log to use</param>
        /// <param name="dataSyncSystemId">The id of the plug-in used when accessing the mapping repository</param>
        /// <param name="webServiceBaseUrl">The base URL of the Spira web service</param>
        /// <param name="internalLogin">The login to Spira</param>
        /// <param name="internalPassword">The password used for the Spira login</param>
        /// <param name="connectionString">The URL for accessing the external system</param>
        /// <param name="externalLogin">The login used for accessing the external system</param>
        /// <param name="externalPassword">The password for the external system</param>
        /// <param name="timeOffsetHours">Any time offset to apply between Spira and the external system</param>
        /// <param name="autoMapUsers">Should we auto-map users</param>
        /// <param name="custom01">Custom configuration 01</param>
        /// <param name="custom02">Custom configuration 02</param>
        /// <param name="custom03">Custom configuration 03</param>
        /// <param name="custom04">Custom configuration 04</param>
        /// <param name="custom05">Custom configuration 05</param>
        public void Setup(
            EventLog eventLog,
            bool traceLogging,
            int dataSyncSystemId,
            string webServiceBaseUrl,
            string internalLogin,
            string internalPassword,
            string connectionString,
            string externalLogin,
            string externalPassword,
            int timeOffsetHours,
            bool autoMapUsers,
            string custom01,
            string custom02,
            string custom03,
            string custom04,
            string custom05
            )
        {
            //Make sure the object has not been already disposed
            if (this.disposed)
            {
                throw new ObjectDisposedException(DATA_SYNC_NAME + " has been disposed already.");
            }

            try
            {
                //Set the member variables from the passed-in values
                this.eventLog = eventLog;
                this.traceLogging = traceLogging;
                this.dataSyncSystemId = dataSyncSystemId;
                this.webServiceBaseUrl = webServiceBaseUrl;
                this.internalLogin = internalLogin;
                this.internalPassword = internalPassword;
                this.connectionString = connectionString;
                this.externalLogin = externalLogin;
                this.externalPassword = externalPassword;
                this.timeOffsetHours = timeOffsetHours;
                this.autoMapUsers = autoMapUsers;
                this.createNewItemsInSpira = (custom01 != null && custom01.ToLowerInvariant() == "false") ? false : true;
                this.createNewItemsInRedmine = (custom02 != null && custom02.ToLowerInvariant() == "false") ? false : true;
                this.custom03 = custom03;
                this.custom04 = custom04;
                this.custom05 = custom05;
            }
            catch (Exception exception)
            {
                //Log and rethrow the exception
                eventLog.WriteEntry("Unable to setup the " + DATA_SYNC_NAME + " plug-in ('" + exception.Message + "')\n" + exception.StackTrace, EventLogEntryType.Error);
                throw exception;
            }
        }

        /// <summary>
        /// Executes the data-sync functionality between the two systems
        /// </summary>
        /// <param name="LastSyncDate">The last date/time the plug-in was successfully executed (in UTC)</param>
        /// <param name="serverDateTime">The current date/time on the server (in UTC)</param>
        /// <returns>Code denoting success, failure or warning</returns>
        public ServiceReturnType Execute(DateTime? lastSyncDate, DateTime serverDateTime)
        {
            //Make sure the object has not been already disposed
            if (this.disposed)
            {
                throw new ObjectDisposedException(DATA_SYNC_NAME + " has been disposed already.");
            }

            try
            {
                LogTraceEvent(eventLog, "Starting " + DATA_SYNC_NAME + " data synchronization", EventLogEntryType.Information);

                //Instantiate the SpiraTest web-service proxy class
                Uri spiraUri = new Uri(this.webServiceBaseUrl + Constants.WEB_SERVICE_URL_SUFFIX);
                SpiraImportExport.ImportExportClient spiraImportExport = SpiraClientFactory.CreateClient(spiraUri);

                //Test that we can connect to redmine by getting something simple (for example, all the projects)
                string apiUrl = this.connectionString;
                RedmineManager redmineManager = new RedmineManager(apiUrl, this.externalLogin, this.externalPassword, MimeFormat.xml, false);
                redmineManager.EventLog = this.eventLog;
                redmineManager.TraceLogging = this.traceLogging;
                try
                {
                    NameValueCollection parameters = new NameValueCollection();
                    IList<Project> projects = redmineManager.GetObjectList<Project>(parameters);
                    if (projects == null || projects.Count < 1)
                    {
                        LogErrorEvent("Unable to connect to " + EXTERNAL_SYSTEM_NAME + " with API at URL '" + apiUrl + "' with login '" + this.externalLogin + "', stopping data-synchronization", EventLogEntryType.Error);
                        return ServiceReturnType.Error;
                    }
                }
                catch (Exception exception)
                {
                    LogErrorEvent("Unable to connect to " + EXTERNAL_SYSTEM_NAME + " with API at URL '" + apiUrl + "' with login '" + this.externalLogin + "', stopping data-synchronization. Error: " + exception.Message, EventLogEntryType.Error);
                    return ServiceReturnType.Error;
                }

                //Now lets get the product name we should be referring to
                string productName = spiraImportExport.System_GetProductName();

                //**** Next lets load in the project and user mappings ****
                bool success = spiraImportExport.Connection_Authenticate2(internalLogin, internalPassword, DATA_SYNC_NAME);
                if (!success)
                {
                    //We can't authenticate so end
                    LogErrorEvent("Unable to authenticate with " + productName + " API, stopping data-synchronization", EventLogEntryType.Error);
                    return ServiceReturnType.Error;
                }
                RemoteDataMapping[] projectMappings = spiraImportExport.DataMapping_RetrieveProjectMappings(dataSyncSystemId);
                RemoteDataMapping[] userMappings = spiraImportExport.DataMapping_RetrieveUserMappings(dataSyncSystemId);

                //Loop for each of the projects in the project mapping
                foreach (RemoteDataMapping projectMapping in projectMappings)
                {
                    //Get the SpiraTest project id equivalent external system project identifier
                    int projectId = projectMapping.InternalId;
                    string externalProjectIdentifier = projectMapping.ExternalKey;

                    //Connect to the SpiraTest project
                    success = spiraImportExport.Connection_ConnectToProject(projectId);
                    if (!success)
                    {
                        //We can't connect so go to next project
                        LogErrorEvent(String.Format("Unable to connect to {0} project PR{1}, please check that the {0} login has the appropriate permissions", productName, projectId), EventLogEntryType.Error);
                        continue;
                    }

                    //Get the list of project-specific mappings from the data-mapping repository
                    //We need to get severity, priority, status and type mappings
                    RemoteDataMapping[] severityMappings = spiraImportExport.DataMapping_RetrieveFieldValueMappings(dataSyncSystemId, (int)Constants.ArtifactField.Severity);
                    RemoteDataMapping[] priorityMappings = spiraImportExport.DataMapping_RetrieveFieldValueMappings(dataSyncSystemId, (int)Constants.ArtifactField.Priority);
                    RemoteDataMapping[] statusMappings = spiraImportExport.DataMapping_RetrieveFieldValueMappings(dataSyncSystemId, (int)Constants.ArtifactField.Status);
                    RemoteDataMapping[] typeMappings = spiraImportExport.DataMapping_RetrieveFieldValueMappings(dataSyncSystemId, (int)Constants.ArtifactField.Type);

                    //Get the list of custom properties configured for this project and the corresponding data mappings
                    RemoteCustomProperty[] incidentCustomProperties = spiraImportExport.CustomProperty_RetrieveForArtifactType((int)Constants.ArtifactType.Incident, false);
                    Dictionary<int, RemoteDataMapping> customPropertyMappingList = new Dictionary<int, RemoteDataMapping>();
                    Dictionary<int, RemoteDataMapping[]> customPropertyValueMappingList = new Dictionary<int, RemoteDataMapping[]>();
                    foreach (RemoteCustomProperty customProperty in incidentCustomProperties)
                    {
                        //Get the mapping for this custom property
                        if (customProperty.CustomPropertyId.HasValue)
                        {
                            RemoteDataMapping customPropertyMapping = spiraImportExport.DataMapping_RetrieveCustomPropertyMapping(dataSyncSystemId, (int)Constants.ArtifactType.Incident, customProperty.CustomPropertyId.Value);
                            customPropertyMappingList.Add(customProperty.CustomPropertyId.Value, customPropertyMapping);

                            //For list types need to also get the property value mappings
                            if (customProperty.CustomPropertyTypeId == (int)Constants.CustomPropertyType.List || customProperty.CustomPropertyTypeId == (int)Constants.CustomPropertyType.MultiList)
                            {
                                RemoteDataMapping[] customPropertyValueMappings = spiraImportExport.DataMapping_RetrieveCustomPropertyValueMappings(dataSyncSystemId, (int)Constants.ArtifactType.Incident, customProperty.CustomPropertyId.Value);
                                customPropertyValueMappingList.Add(customProperty.CustomPropertyId.Value, customPropertyValueMappings);
                            }
                        }
                    }

                    //Now get the list of releases and incidents that have already been mapped
                    RemoteDataMapping[] incidentMappings = spiraImportExport.DataMapping_RetrieveArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Incident);
                    RemoteDataMapping[] releaseMappings = spiraImportExport.DataMapping_RetrieveArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Release);

                    NameValueCollection parameters = new NameValueCollection();
                    Project project = null;

                    try
                    {
                        project = redmineManager.GetObject<Project>(externalProjectIdentifier, parameters);
                    }
                    catch (Exception exception)
                    {
                        //Log and leave null so we skip the project
                        LogErrorEvent(exception.Message, EventLogEntryType.Error);
                    }

                    if (project == null)
                    {
                        LogErrorEvent(String.Format("Unable to retrieve project '{0}' from {1}, skipping this project. Make sure that the Redmine project external key is the correct Redmine project identifier (not the display name!)", externalProjectIdentifier, EXTERNAL_SYSTEM_NAME), EventLogEntryType.Error);
                        continue;
                    }
                    int externalProjectId = project.Id;
                    LogTraceEvent(eventLog, String.Format("Successfully retrieved project '{0}' from {1}", project.Name, EXTERNAL_SYSTEM_NAME), EventLogEntryType.Information);

                    //**** First we need to get the list of recently created incidents in SpiraTest ****

                    //If we don't have a last-sync data, default to 1/1/1950
                    if (!lastSyncDate.HasValue)
                    {
                        lastSyncDate = DateTime.ParseExact("1/1/1950", "M/d/yyyy", CultureInfo.InvariantCulture);
                    }

                    //Create the mapping collections to hold any new items that need to get added to the mappings
                    //or any old items that need to get removed from the mappings
                    List<RemoteDataMapping> newIncidentMappings = new List<RemoteDataMapping>();
                    List<RemoteDataMapping> newReleaseMappings = new List<RemoteDataMapping>();
                    List<RemoteDataMapping> oldReleaseMappings = new List<RemoteDataMapping>();

                    if (this.createNewItemsInRedmine)
                    {
                        //Get the incidents in batches of 100
                        List<RemoteIncident> incidentList = new List<RemoteIncident>();
                        long incidentCount = spiraImportExport.Incident_Count(null);
                        for (int startRow = 1; startRow <= incidentCount; startRow += Constants.INCIDENT_PAGE_SIZE)
                        {
                            RemoteIncident[] incidentBatch = spiraImportExport.Incident_RetrieveNew(lastSyncDate.Value, startRow, Constants.INCIDENT_PAGE_SIZE);
                            incidentList.AddRange(incidentBatch);
                        }
                        LogTraceEvent(eventLog, "Found " + incidentList.Count + " new incidents in " + productName, EventLogEntryType.Information);

                        //Iterate through each new Spira incident record and add to the external system
                        foreach (RemoteIncident remoteIncident in incidentList)
                        {
                            try
                            {
                                ProcessIncident(projectId, spiraImportExport, redmineManager, remoteIncident, newIncidentMappings, newReleaseMappings, oldReleaseMappings, customPropertyMappingList, customPropertyValueMappingList, incidentCustomProperties, incidentMappings, externalProjectId, productName, severityMappings, priorityMappings, statusMappings, typeMappings, userMappings, releaseMappings);
                            }
                            catch (Exception exception)
                            {
                                //Log and continue execution
                                LogErrorEvent("Error Adding " + productName + " Incident to " + EXTERNAL_SYSTEM_NAME + ": " + exception.Message + "\n" + exception.StackTrace, EventLogEntryType.Error);
                            }
                        }

                        //Finally we need to update the mapping data on the server before starting the second phase
                        //of the data-synchronization
                        //At this point we have potentially added incidents, added releases and removed releases
                        spiraImportExport.DataMapping_AddArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Incident, newIncidentMappings.ToArray());
                        spiraImportExport.DataMapping_AddArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Release, newReleaseMappings.ToArray());
                        spiraImportExport.DataMapping_RemoveArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Release, oldReleaseMappings.ToArray());
                    }

                    //Re-authenticate with Spira and reconnect to the project to avoid potential timeout issues
                    success = spiraImportExport.Connection_Authenticate2(internalLogin, internalPassword, DATA_SYNC_NAME);
                    if (!success)
                    {
                        //We can't authenticate so end
                        LogErrorEvent("Unable to authenticate with " + productName + " API, stopping data-synchronization", EventLogEntryType.Error);
                        return ServiceReturnType.Error;
                    }
                    success = spiraImportExport.Connection_ConnectToProject(projectId);
                    if (!success)
                    {
                        //We can't connect so go to next project
                        LogErrorEvent("Unable to connect to " + productName + " project PR" + projectId + ", please check that the " + productName + " login has the appropriate permissions", EventLogEntryType.Error);
                        return ServiceReturnType.Error;
                    }

                    //**** Next we need to see if any of the previously mapped incidents has changed or any new items added to the external system ****
                    incidentMappings = spiraImportExport.DataMapping_RetrieveArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Incident);

                    //Need to create a list to hold any new releases and new incidents
                    newIncidentMappings = new List<RemoteDataMapping>();
                    newReleaseMappings = new List<RemoteDataMapping>();

                    //Call the External System API to get the list of recently added/changed issues
                    //i.e. issues that have a last updated date >= filterDate and are in the appropriate project
                    DateTime filterDate = lastSyncDate.Value.AddHours(-timeOffsetHours);
                    if (filterDate < DateTime.Parse("1/1/1990"))
                    {
                        filterDate = DateTime.Parse("1/1/1990");
                    }

                    List<Issue> redmineIssues = new List<Issue>();
                    bool emptyListReturned = false;
                    for (int offset = 0; !emptyListReturned; offset += Constants.INCIDENT_PAGE_SIZE)
                    {
                        parameters.Clear();
                        parameters.Add("project_id", externalProjectId.ToString());
                        parameters.Add("updated_on", String.Format(">={0:yyyy-MM-dd}", filterDate));
                        parameters.Add("offset", offset.ToString());
                        parameters.Add("status_id", "*");   //Gets open & closed issues
                        parameters.Add("limit", Constants.INCIDENT_PAGE_SIZE.ToString());
                        IList<Issue> issueBatch = redmineManager.GetObjectList<Issue>(parameters);
                        if (issueBatch == null || issueBatch.Count < 1)
                        {
                            emptyListReturned = true;
                        }
                        else
                        {
                            redmineIssues.AddRange(issueBatch);
                        }
                    }
                    LogTraceEvent(eventLog, "Found " + redmineIssues.Count + " new issues in " + EXTERNAL_SYSTEM_NAME, EventLogEntryType.Information);

                    //Iterate through these items
                    parameters.Clear();
                    parameters.Add("include", "attachments");
                    parameters.Add("include", "relations");
                    parameters.Add("include", "journals");
                    foreach (Issue redmineIssue in redmineIssues)
                    {
                        try
                        {
                            //Retrieve the redmine issue with all the additional data
                            Issue redmineIssueDetails = redmineManager.GetObject<Issue>(redmineIssue.Id.ToString(), parameters);

                            //Extract the data from the external issue object and load into Spira as a new incident
                            ProcessExternalIssue(projectId, spiraImportExport, redmineManager, redmineIssueDetails, newIncidentMappings, newReleaseMappings, oldReleaseMappings, customPropertyMappingList, customPropertyValueMappingList, incidentCustomProperties, incidentMappings, externalProjectId, productName, severityMappings, priorityMappings, statusMappings, typeMappings, userMappings, releaseMappings);
                        }
                        catch (RedmineDeserializationException exception)
                        {
                            //Log and continue execution
                            LogErrorEvent("Error Deserializing " + EXTERNAL_SYSTEM_NAME + " issue " + redmineIssue.Id + ": " + exception.Message + "\n" + exception.StackTrace, EventLogEntryType.Error);
                            LogErrorEvent("Response XML: " + exception.Xml, EventLogEntryType.Error);
                        }
                        catch (Exception exception)
                        {
                            //Log and continue execution
                            LogErrorEvent("Error Inserting/Updating " + EXTERNAL_SYSTEM_NAME + " issue in " + productName + ": " + exception.Message + "\n" + exception.StackTrace, EventLogEntryType.Error);
                        }
                    }

                    //Finally we need to update the mapping data on the server
                    //At this point we have potentially added releases and incidents
                    spiraImportExport.DataMapping_AddArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Release, newReleaseMappings.ToArray());
                    spiraImportExport.DataMapping_AddArtifactMappings(dataSyncSystemId, (int)Constants.ArtifactType.Incident, newIncidentMappings.ToArray());
                }

                //The following code is only needed during debugging
                LogTraceEvent(eventLog, "Import Completed", EventLogEntryType.Warning);

                //Mark objects ready for garbage collection
                spiraImportExport = null;
                redmineManager = null;

                //Let the service know that we ran correctly
                return ServiceReturnType.Success;
            }
            catch (Exception exception)
            {
                //Log the exception and return as a failure
                LogErrorEvent("General Error: " + exception.Message + "\n" + exception.StackTrace, EventLogEntryType.Error);
                return ServiceReturnType.Error;
            }
        }

        /// <summary>
        /// Processes a single SpiraTest incident record and adds to the external system
        /// </summary>
        /// <param name="projectId">The id of the current project</param>
        /// <param name="spiraImportExport">The Spira API proxy class</param>
        /// <param name="remoteIncident">The Spira incident</param>
        /// <param name="newIncidentMappings">The list of any new incidents to be mapped</param>
        /// <param name="newReleaseMappings">The list of any new releases to be mapped</param>
        /// <param name="oldReleaseMappings">The list list of old releases to be un-mapped</param>
        /// <param name="customPropertyMappingList">The mapping of custom properties</param>
        /// <param name="customPropertyValueMappingList">The mapping of custom property list values</param>
        /// <param name="incidentCustomProperties">The list of incident custom properties defined for this project</param>
        /// <param name="incidentMappings">The list of existing mapped incidents</param>
        /// <param name="externalProjectId">The id of the project in the external system</param>
        /// <param name="productName">The name of the product being connected to (SpiraTest, SpiraPlan, etc.)</param>
        /// <param name="severityMappings">The incident severity mappings</param>
        /// <param name="priorityMappings">The incident priority mappings</param>
        /// <param name="statusMappings">The incident status mappings</param>
        /// <param name="typeMappings">The incident type mappings</param>
        /// <param name="userMappings">The incident user mappings</param>
        /// <param name="releaseMappings">The release mappings</param>
        /// <param name="redmineManager">The redmine client proxy</param>
        private void ProcessIncident(int projectId, ImportExportClient spiraImportExport, RedmineManager redmineManager, RemoteIncident remoteIncident, List<RemoteDataMapping> newIncidentMappings, List<RemoteDataMapping> newReleaseMappings, List<RemoteDataMapping> oldReleaseMappings, Dictionary<int, RemoteDataMapping> customPropertyMappingList, Dictionary<int, RemoteDataMapping[]> customPropertyValueMappingList, RemoteCustomProperty[] incidentCustomProperties, RemoteDataMapping[] incidentMappings, int externalProjectId, string productName, RemoteDataMapping[] severityMappings, RemoteDataMapping[] priorityMappings, RemoteDataMapping[] statusMappings, RemoteDataMapping[] typeMappings, RemoteDataMapping[] userMappings, RemoteDataMapping[] releaseMappings)
        {
            //Get certain incident fields into local variables (if used more than once)
            int incidentId = remoteIncident.IncidentId.Value;
            int incidentStatusId = remoteIncident.IncidentStatusId.Value;

            //Make sure we've not already loaded this issue
            if (InternalFunctions.FindMappingByInternalId(projectId, incidentId, incidentMappings) == null)
            {
                //Get the URL for the incident in Spira, we'll use it later
                string baseUrl = spiraImportExport.System_GetWebServerUrl();
                string incidentUrl = spiraImportExport.System_GetArtifactUrl((int)Constants.ArtifactType.Incident, projectId, incidentId, "").Replace("~", baseUrl);

                //Get the name/description of the incident. The description will be available in both rich (HTML) and plain-text
                //depending on what the external system can handle
                string externalName = remoteIncident.Name;
                string externalDescriptionHtml = remoteIncident.Description;
                string externalDescriptionPlainText = InternalFunctions.HtmlRenderAsPlainText(externalDescriptionHtml);

                //See if this incident has any associations
                RemoteSort associationSort = new RemoteSort();
                associationSort.SortAscending = true;
                associationSort.PropertyName = "CreationDate";
                RemoteAssociation[] remoteAssociations = spiraImportExport.Association_RetrieveForArtifact((int)Constants.ArtifactType.Incident, incidentId, null, associationSort);

                //See if this incident has any attachments
                RemoteSort attachmentSort = new RemoteSort();
                attachmentSort.SortAscending = true;
                attachmentSort.PropertyName = "AttachmentId";
                RemoteDocument[] remoteDocuments = spiraImportExport.Document_RetrieveForArtifact((int)Constants.ArtifactType.Incident, incidentId, null, attachmentSort);

                //Get some of the incident's non-mappable fields
                DateTime creationDate = remoteIncident.CreationDate.Value;
                DateTime lastUpdateDate = remoteIncident.LastUpdateDate;
                DateTime? startDate = remoteIncident.StartDate;
                DateTime? closedDate = remoteIncident.ClosedDate;
                int? estimatedEffortInMinutes = remoteIncident.EstimatedEffort;
                int? actualEffortInMinutes = remoteIncident.ActualEffort;
                int? projectedEffortInMinutes = remoteIncident.ProjectedEffort;
                int? remainingEffortInMinutes = remoteIncident.RemainingEffort;
                int completionPercent = remoteIncident.CompletionPercent;

                //Now get the external system's equivalent incident status from the mapping
                RemoteDataMapping dataMapping = InternalFunctions.FindMappingByInternalId(projectId, remoteIncident.IncidentStatusId.Value, statusMappings);
                if (dataMapping == null)
                {
                    //We can't find the matching item so log and move to the next incident
                    LogErrorEvent("Unable to locate mapping entry for incident status " + remoteIncident.IncidentStatusId + " in project " + projectId, EventLogEntryType.Error);
                    return;
                }
                string externalStatus = dataMapping.ExternalKey;

                //Now get the external system's equivalent incident type from the mapping
                dataMapping = InternalFunctions.FindMappingByInternalId(projectId, remoteIncident.IncidentTypeId.Value, typeMappings);
                if (dataMapping == null)
                {
                    //We can't find the matching item so log and move to the next incident
                    LogErrorEvent("Unable to locate mapping entry for incident type " + remoteIncident.IncidentTypeId + " in project " + projectId, EventLogEntryType.Error);
                    return;
                }
                string externalType = dataMapping.ExternalKey;

                //Now get the external system's equivalent priority from the mapping (if priority is set)
                string externalPriority = "";
                if (remoteIncident.PriorityId.HasValue)
                {
                    dataMapping = InternalFunctions.FindMappingByInternalId(projectId, remoteIncident.PriorityId.Value, priorityMappings);
                    if (dataMapping == null)
                    {
                        //We can't find the matching item so log and just don't set the priority
                        LogErrorEvent("Unable to locate mapping entry for incident priority " + remoteIncident.PriorityId.Value + " in project " + projectId, EventLogEntryType.Warning);
                    }
                    else
                    {
                        externalPriority = dataMapping.ExternalKey;
                    }
                }

                //Now get the external system's equivalent severity from the mapping (if severity is set)
                string externalSeverity = "";
                if (remoteIncident.SeverityId.HasValue)
                {
                    dataMapping = InternalFunctions.FindMappingByInternalId(projectId, remoteIncident.SeverityId.Value, severityMappings);
                    if (dataMapping == null)
                    {
                        //We can't find the matching item so log and just don't set the severity
                        LogErrorEvent("Unable to locate mapping entry for incident severity " + remoteIncident.SeverityId.Value + " in project " + projectId, EventLogEntryType.Warning);
                    }
                    else
                    {
                        externalSeverity = dataMapping.ExternalKey;
                    }
                }

                //Now get the external system's ID for the Opener/Detector of the incident (reporter)
                string externalReporter = "";
                dataMapping = FindUserMappingByInternalId(remoteIncident.OpenerId.Value, userMappings, spiraImportExport, redmineManager);
                //If we can't find the user, just log a warning
                if (dataMapping == null)
                {
                    LogErrorEvent("Unable to locate mapping entry for user id " + remoteIncident.OpenerId.Value + " so using synchronization user", EventLogEntryType.Warning);
                }
                else
                {
                    externalReporter = dataMapping.ExternalKey;
                }

                //Now get the external system's ID for the Owner of the incident (assignee)
                string externalAssignee = "";
                if (remoteIncident.OwnerId.HasValue)
                {
                    dataMapping = FindUserMappingByInternalId(remoteIncident.OwnerId.Value, userMappings, spiraImportExport, redmineManager);
                    //If we can't find the user, just log a warning
                    if (dataMapping == null)
                    {
                        LogErrorEvent("Unable to locate mapping entry for user id " + remoteIncident.OwnerId.Value + " so leaving assignee empty", EventLogEntryType.Warning);
                    }
                    else
                    {
                        externalAssignee = dataMapping.ExternalKey;
                    }
                }

                /* Redmine does not have a detected release, only a resolved one, so we add any new releases, but we don't set a value on the new issue */
                string externalDetectedRelease = "";
                if (remoteIncident.DetectedReleaseId.HasValue)
                {
                    int detectedReleaseId = remoteIncident.DetectedReleaseId.Value;
                    dataMapping = InternalFunctions.FindMappingByInternalId(projectId, detectedReleaseId, releaseMappings);
                    if (dataMapping == null)
                    {
                        //We can't find the matching item so need to create a new version in the external system and add to mappings
                        //Since version numbers are now unique in both systems, we can simply use that
                        LogTraceEvent(eventLog, "Adding new release in " + EXTERNAL_SYSTEM_NAME + " for release " + detectedReleaseId + "\n", EventLogEntryType.Information);

                        //Get the Spira release
                        RemoteRelease remoteRelease = spiraImportExport.Release_RetrieveById(detectedReleaseId);
                        if (remoteRelease != null)
                        {
                            try
                            {
                                //Add the redmine version
                                Redmine.Net.Api.Types.Version version = new Redmine.Net.Api.Types.Version();
                                version.Name = remoteIncident.DetectedReleaseVersionNumber;
                                version.Project.Id = externalProjectId;
                                version.Status = VersionStatus.open;
                                version = redmineManager.CreateObject<Redmine.Net.Api.Types.Version>(version, externalProjectId.ToString());
                                externalDetectedRelease = version.Id.ToString();

                                //Add a new mapping entry
                                RemoteDataMapping newReleaseMapping = new RemoteDataMapping();
                                newReleaseMapping.ProjectId = projectId;
                                newReleaseMapping.InternalId = detectedReleaseId;
                                newReleaseMapping.ExternalKey = externalDetectedRelease;
                                newReleaseMappings.Add(newReleaseMapping);
                            }
                            catch (Exception exception)
                            {
                                LogErrorEvent("Error adding new release in " + EXTERNAL_SYSTEM_NAME + " for release " + detectedReleaseId + " - " + exception.Message, EventLogEntryType.Error);
                                throw;
                            }
                        }
                    }
                    else
                    {
                        externalDetectedRelease = dataMapping.ExternalKey;
                    }

                //    //Verify that this release still exists in the external system
                //    LogTraceEvent(eventLog, "Looking for " + EXTERNAL_SYSTEM_NAME + " detected release: " + externalDetectedRelease + "\n", EventLogEntryType.Information);

                //    /*
                //     * Set the value of the matchFound flag based on whether the external release exists
                //     */

                //    bool matchFound = false;
                //    if (matchFound)
                //    {
                //        //Set the externalRelease value on the external incident
                //    }
                //    else
                //    {
                //        //We can't find the matching item so log and just don't set the release
                //        LogErrorEvent("Unable to locate " + EXTERNAL_SYSTEM_NAME + " detected release " + externalDetectedRelease + " in project " + externalProjectId, EventLogEntryType.Warning);

                //        //Add this to the list of mappings to remove
                //        RemoteDataMapping oldReleaseMapping = new RemoteDataMapping();
                //        oldReleaseMapping.ProjectId = projectId;
                //        oldReleaseMapping.InternalId = detectedReleaseId;
                //        oldReleaseMapping.ExternalKey = externalDetectedRelease;
                //        oldReleaseMappings.Add(oldReleaseMapping);
                //    }
                }
                //LogTraceEvent(eventLog, "Set " + EXTERNAL_SYSTEM_NAME + " detected release\n", EventLogEntryType.Information);

                //Specify the resolved-in version/release if applicable
                string externalResolvedRelease = "";
                if (remoteIncident.ResolvedReleaseId.HasValue)
                {
                    int resolvedReleaseId = remoteIncident.ResolvedReleaseId.Value;
                    dataMapping = InternalFunctions.FindMappingByInternalId(projectId, resolvedReleaseId, releaseMappings);
                    if (dataMapping == null)
                    {
                        //We can't find the matching item so need to create a new version in the external system and add to mappings
                        //Since version numbers are now unique in both systems, we can simply use that
                        LogTraceEvent(eventLog, "Adding new release in " + EXTERNAL_SYSTEM_NAME + " for release " + resolvedReleaseId + "\n", EventLogEntryType.Information);

                        //Get the Spira release
                        RemoteRelease remoteRelease = spiraImportExport.Release_RetrieveById(resolvedReleaseId);
                        if (remoteRelease != null)
                        {
                            try
                            {
                                //Add the redmine version
                                Redmine.Net.Api.Types.Version version = new Redmine.Net.Api.Types.Version();
                                version.Name = remoteIncident.ResolvedReleaseVersionNumber;
                                version.Project.Id = externalProjectId;
                                version.Status = VersionStatus.open;
                                version = redmineManager.CreateObject<Redmine.Net.Api.Types.Version>(version, externalProjectId.ToString());
                                externalResolvedRelease = version.Id.ToString();

                                //Add a new mapping entry
                                RemoteDataMapping newReleaseMapping = new RemoteDataMapping();
                                newReleaseMapping.ProjectId = projectId;
                                newReleaseMapping.InternalId = resolvedReleaseId;
                                newReleaseMapping.ExternalKey = externalResolvedRelease;
                                newReleaseMappings.Add(newReleaseMapping);
                            }
                            catch (Exception exception)
                            {
                                LogErrorEvent("Error adding new release in " + EXTERNAL_SYSTEM_NAME + " for release " + resolvedReleaseId + " - " + exception.Message, EventLogEntryType.Error);
                                throw;
                            }
                        }
                    }
                    else
                    {
                        externalResolvedRelease = dataMapping.ExternalKey;
                    }

                    //Verify that this release still exists in the external system
                    LogTraceEvent(eventLog, "Verifying " + EXTERNAL_SYSTEM_NAME + " resolved release: " + externalResolvedRelease + "\n", EventLogEntryType.Information);

                    NameValueCollection parameters = new NameValueCollection();
                    Redmine.Net.Api.Types.Version version2 = redmineManager.GetObject<Redmine.Net.Api.Types.Version>(externalResolvedRelease, parameters);
                    bool matchFound = (version2 != null);

                    if (!matchFound)
                    {
                        //We can't find the matching item so log and just don't set the release
                        externalResolvedRelease = "";
                        LogErrorEvent("Unable to locate " + EXTERNAL_SYSTEM_NAME + " resolved release " + externalResolvedRelease + " in project " + externalProjectId, EventLogEntryType.Warning);

                        //Add this to the list of mappings to remove
                        RemoteDataMapping oldReleaseMapping = new RemoteDataMapping();
                        oldReleaseMapping.ProjectId = projectId;
                        oldReleaseMapping.InternalId = resolvedReleaseId;
                        oldReleaseMapping.ExternalKey = externalResolvedRelease;
                        oldReleaseMappings.Add(oldReleaseMapping);
                    }
                }
                LogTraceEvent(eventLog, "Set " + EXTERNAL_SYSTEM_NAME + " resolved release\n", EventLogEntryType.Information);

                //Setup the list to hold the various custom properties to set on the external bug system
                List<CustomField> externalSystemCustomFieldValues = new List<CustomField>();

                //Now we need to see if any of the custom properties have changed
                if (remoteIncident.CustomProperties != null && remoteIncident.CustomProperties.Length > 0)
                {
                    ProcessCustomProperties(productName, projectId, remoteIncident, externalSystemCustomFieldValues, customPropertyMappingList, customPropertyValueMappingList, userMappings, spiraImportExport);
                }
                LogTraceEvent(eventLog, "Captured incident custom values\n", EventLogEntryType.Information);

                /*
                 * Create the incident in the external system using the following values
                 *  - externalName
                 *  - externalDescriptionHtml
                 *  - externalDescriptionPlainText
                 *  - externalProjectId
                 *  - externalStatus
                 *  - externalType
                 *  - externalPriority
                 *  - externalSeverity
                 *  - externalReporter
                 *  - externalAssignee
                 *  - externalDetectedRelease
                 *  - externalResolvedRelease
                 *  - externalSystemCustomFieldValues
                 *  - startDate
                 *  - closedDate
                 *  - creationDate
                 *  - lastUpdateDate
                 *  - estimatedEffortInMinutes
                 *  - actualEffortInMinutes
                 *  - projectedEffortInMinutes
                 *  - remainingEffortInMinutes
                 *  - completionPercent
                 *  
                 */
                Issue issue = new Issue();
                issue.Subject = externalName;
                issue.Description = externalDescriptionPlainText;
                issue.CreatedOn = creationDate;
                issue.UpdatedOn = lastUpdateDate;
                issue.Project = new IdentifiableName();
                issue.Project.Id = externalProjectId;
                if (!String.IsNullOrEmpty(externalStatus))
                {
                    issue.Status = new IdentifiableName();
                    issue.Status.Id = InternalFunctions.ToSafeInt32(externalStatus, "Status");
                }
                if (!String.IsNullOrEmpty(externalType))
                {
                    issue.Tracker = new IdentifiableName();
                    issue.Tracker.Id = InternalFunctions.ToSafeInt32(externalType, "Tracker");
                }
                if (!String.IsNullOrEmpty(externalReporter))
                {
                    issue.Author = new IdentifiableName();
                    issue.Author.Id = InternalFunctions.ToSafeInt32(externalReporter, "Author");
                }
                issue.DueDate = closedDate;
                issue.StartDate = startDate;
                if (!String.IsNullOrEmpty(externalResolvedRelease))
                {
                    issue.FixedVersion = new IdentifiableName();
                    issue.FixedVersion.Id = InternalFunctions.ToSafeInt32(externalResolvedRelease, "FixedVersion");
                }
                if (estimatedEffortInMinutes.HasValue)
                {
                    issue.EstimatedHours = ((float)estimatedEffortInMinutes.Value) / (float)60;
                }
                issue.DoneRatio = (float)completionPercent;
                if (!String.IsNullOrEmpty(externalPriority))
                {
                    issue.Priority = new IdentifiableName();
                    issue.Priority.Id = InternalFunctions.ToSafeInt32(externalPriority, "Priority");
                }
                if (!String.IsNullOrEmpty(externalAssignee))
                {
                    issue.AssignedTo = new IdentifiableName();
                    issue.AssignedTo.Id = InternalFunctions.ToSafeInt32(externalAssignee, "AssignedTo");
                }

                //Custom fields
                if (externalSystemCustomFieldValues != null && externalSystemCustomFieldValues.Count > 0)
                {
                    issue.CustomFields = externalSystemCustomFieldValues;
                }

                LogTraceEvent(eventLog, "About to create issue in " + EXTERNAL_SYSTEM_NAME + "\n", EventLogEntryType.Information);
                issue = redmineManager.CreateObject<Issue>(issue);
                string externalBugId = issue.Id.ToString();
                LogTraceEvent(eventLog, "Created issue in " + EXTERNAL_SYSTEM_NAME + ", new issue id=" + externalBugId, EventLogEntryType.Information);

                //Add the external bug id to mappings table
                RemoteDataMapping newIncidentMapping = new RemoteDataMapping();
                newIncidentMapping.ProjectId = projectId;
                newIncidentMapping.InternalId = incidentId;
                newIncidentMapping.ExternalKey = externalBugId;
                newIncidentMappings.Add(newIncidentMapping);

                //We also add a link to the external issue from the Spira incident
                if (!String.IsNullOrEmpty(EXTERNAL_BUG_URL))
                {
                    string externalUrl = connectionString + String.Format(EXTERNAL_BUG_URL, externalBugId);
                    RemoteDocument remoteUrl = new RemoteDocument();
                    remoteUrl.ArtifactId = incidentId;
                    remoteUrl.ArtifactTypeId = (int)Constants.ArtifactType.Incident;
                    remoteUrl.Description = "Link to issue in " + EXTERNAL_SYSTEM_NAME;
                    remoteUrl.FilenameOrUrl = externalUrl;
                    spiraImportExport.Document_AddUrl(remoteUrl);
                }

                //See if we have any comments to add to the external system
                RemoteComment[] incidentComments = spiraImportExport.Incident_RetrieveComments(incidentId);
                if (incidentComments != null)
                {
                    foreach (RemoteComment incidentComment in incidentComments)
                    {
                        string externalResolutionText = incidentComment.Text;
                        creationDate = incidentComment.CreationDate.Value;

                        //Get the id of the corresponding external user that added the comments
                        string externalCommentAuthor = "";
                        dataMapping = InternalFunctions.FindMappingByInternalId(incidentComment.UserId.Value, userMappings);
                        //If we can't find the user, just log a warning
                        if (dataMapping == null)
                        {
                            LogErrorEvent("Unable to locate mapping entry for user id " + incidentComment.UserId.Value + " so using synchronization user", EventLogEntryType.Warning);
                        }
                        else
                        {
                            externalCommentAuthor = dataMapping.ExternalKey;
                        }

                        /*
                         * Add a comment to the external bug-tracking system using the following values
                         *  - externalResolutionText
                         *  - creationDate
                         *  - externalCommentAuthor
                         */
                        issue.Notes = externalResolutionText;
                        redmineManager.UpdateObject<Issue>(externalBugId, issue);
                    }
                }

                //See if we have any attachments to add to the external bug
                if (remoteDocuments != null && remoteDocuments.Length > 0)
                {
                    foreach (RemoteDocument remoteDocument in remoteDocuments)
                    {
                        //See if we have a file attachment or simple URL - currenly Redmine only supports files
                        if (remoteDocument.AttachmentTypeId == (int)Constants.AttachmentType.File)
                        {
                            try
                            {
                                //Get the binary data for the attachment
                                byte[] binaryData = spiraImportExport.Document_OpenFile(remoteDocument.AttachmentId.Value);
                                if (binaryData != null && binaryData.Length > 0)
                                {
                                    //First we need to upload the data
                                    Upload upload = redmineManager.UploadData(binaryData);

                                    //Next we need to attach this to the issue
                                    upload.Description = remoteDocument.Description;
                                    upload.FileName = remoteDocument.FilenameOrUrl;

                                    if (issue.Uploads == null)
                                    {
                                        issue.Uploads = new List<Upload>();
                                    }
                                    issue.Uploads.Add(upload);
                                }
                            }
                            catch (Exception exception)
                            {
                                //Log an error and continue because this can fail if the files are too large
                                LogErrorEvent("Error adding " + productName + " incident attachment DC" + remoteDocument.AttachmentId.Value + " to " + EXTERNAL_SYSTEM_NAME + ": " + exception.Message + "\n. (The issue itself was added.)\n Stack Trace: " + exception.StackTrace, EventLogEntryType.Error);
                            }
                        }
                    }
                    
                    //Actually do the upload
                    try
                    {
                        issue.Notes = "Adding attachmemts from " + productName;
                        redmineManager.UpdateObject<Issue>(externalBugId, issue);
                    }
                    catch (Exception exception)
                    {
                        //Log an error and continue because this can fail if the files are too large
                        LogErrorEvent("Error adding " + productName + " incident attachments to " + EXTERNAL_SYSTEM_NAME + ": " + exception.Message + "\n. (The issue itself was added.)\n Stack Trace: " + exception.StackTrace, EventLogEntryType.Error);
                    }
                }

                //See if we have any incident-to-incident associations to add to the external bug
                if (remoteAssociations != null && remoteAssociations.Length > 0)
                {
                    try
                    {
                        foreach (RemoteAssociation remoteAssociation in remoteAssociations)
                        {
                            //Make sure the linked-to item is an incident
                            if (remoteAssociation.DestArtifactTypeId == (int)Constants.ArtifactType.Incident)
                            {
                                dataMapping = InternalFunctions.FindMappingByInternalId(remoteAssociation.DestArtifactId, incidentMappings);
                                if (dataMapping != null)
                                {
                                    //Add a link in the external system to the following target bug id
                                    string externalTargetBugId = dataMapping.ExternalKey;
                                    IssueRelation issueRelation = new IssueRelation();
                                    issueRelation.IssueId = InternalFunctions.ToSafeInt32(externalBugId);
                                    issueRelation.IssueToId = InternalFunctions.ToSafeInt32(externalTargetBugId);
                                    redmineManager.CreateObject<IssueRelation>(issueRelation, externalBugId);
                                }
                            }
                        }
                    }
                    catch (Exception exception)
                    {
                        //Log an error and continue because this can fail if the files are too large
                        LogErrorEvent("Error adding " + productName + " incident assocations to " + EXTERNAL_SYSTEM_NAME + ": " + exception.Message + "\n. (The issue itself was added.)\n Stack Trace: " + exception.StackTrace, EventLogEntryType.Error);
                    }
                }
            }
        }

        /// <summary>
        /// Processes a single external bug record and either adds or updates it in SpiraTest
        /// </summary>
        /// <param name="projectId">The id of the current project</param>
        /// <param name="spiraImportExport">The Spira API proxy class</param>
        /// <param name="redmineIssue">The external bug object</param>
        /// <param name="newIncidentMappings">The list of any new incidents to be mapped</param>
        /// <param name="newReleaseMappings">The list of any new releases to be mapped</param>
        /// <param name="oldReleaseMappings">The list list of old releases to be un-mapped</param>
        /// <param name="customPropertyMappingList">The mapping of custom properties</param>
        /// <param name="customPropertyValueMappingList">The mapping of custom property list values</param>
        /// <param name="incidentCustomProperties">The list of incident custom properties defined for this project</param>
        /// <param name="incidentMappings">The list of existing mapped incidents</param>
        /// <param name="externalProjectId">The id of the project in the external system</param>
        /// <param name="productName">The name of the product being connected to (SpiraTest, SpiraPlan, etc.)</param>
        /// <param name="severityMappings">The incident severity mappings</param>
        /// <param name="priorityMappings">The incident priority mappings</param>
        /// <param name="statusMappings">The incident status mappings</param>
        /// <param name="typeMappings">The incident type mappings</param>
        /// <param name="userMappings">The incident user mappings</param>
        /// <param name="releaseMappings">The release mappings</param>
        /// <param name="redmineManager">The redmine api manager</param>
        private void ProcessExternalIssue(int projectId, ImportExportClient spiraImportExport, RedmineManager redmineManager, Issue redmineIssue, List<RemoteDataMapping> newIncidentMappings, List<RemoteDataMapping> newReleaseMappings, List<RemoteDataMapping> oldReleaseMappings, Dictionary<int, RemoteDataMapping> customPropertyMappingList, Dictionary<int, RemoteDataMapping[]> customPropertyValueMappingList, RemoteCustomProperty[] incidentCustomProperties, RemoteDataMapping[] incidentMappings, int externalProjectId, string productName, RemoteDataMapping[] severityMappings, RemoteDataMapping[] priorityMappings, RemoteDataMapping[] statusMappings, RemoteDataMapping[] typeMappings, RemoteDataMapping[] userMappings, RemoteDataMapping[] releaseMappings)
        {
            string externalBugId = redmineIssue.Id.ToString();
            string externalBugName = redmineIssue.Subject;
            string externalBugDescription = redmineIssue.Description;
            int externalBugProjectId = redmineIssue.Project.Id;
            string externalBugCreator = redmineIssue.Author.Id.ToString();
            string externalBugPriority = "";
            if (redmineIssue.Priority != null)
            {
                externalBugPriority = redmineIssue.Priority.Id.ToString();
            }
            string externalBugSeverity = "";    //Not supported by redmine
            string externalBugStatus = redmineIssue.Status.Id.ToString();
            string externalBugType = redmineIssue.Tracker.Id.ToString();
            string externalBugAssignee = "";
            if (redmineIssue.AssignedTo != null)
            {
                externalBugAssignee = redmineIssue.AssignedTo.Id.ToString();
            }
            string externalBugDetectedRelease = "";    //Not supported by redmine
            string externalBugResolvedRelease = "";
            string externalBugResolvedReleaseName = "";
            if (redmineIssue.FixedVersion != null)
            {
                externalBugResolvedRelease = redmineIssue.FixedVersion.Id.ToString();
                if (String.IsNullOrEmpty(redmineIssue.FixedVersion.Name))
                {
                    externalBugResolvedReleaseName = externalBugResolvedRelease;
                }
                else
                {
                    externalBugResolvedReleaseName = redmineIssue.FixedVersion.Name;
                }
            }
            DateTime? externalBugStartDate = redmineIssue.StartDate;
            DateTime? externalBugClosedDate = redmineIssue.DueDate;
            int? externalEstimatedEffortInMinutes = null;
            if (redmineIssue.EstimatedHours.HasValue)
            {
                externalEstimatedEffortInMinutes = (int)(redmineIssue.EstimatedHours.Value * (float)60);
            }
            int? externalRemainingEffortInMinutes = null;
            if (redmineIssue.EstimatedHours.HasValue && redmineIssue.DoneRatio.HasValue)
            {
                float estimatedHours = redmineIssue.EstimatedHours.Value;
                float doneHours = (redmineIssue.DoneRatio.Value * estimatedHours) / (float)100;
                float remainingHours = estimatedHours - doneHours;
                if (remainingHours > 0)
                {
                    externalRemainingEffortInMinutes = (int)(remainingHours * (float)60);
                }
            }

            //Make sure the projects match (i.e. the external bug is in the project being synced)
            //It should be handled previously in the filter sent to external system, but use this as an extra check
            if (externalBugProjectId == externalProjectId)
            {
                //See if we have an existing mapping or not
                RemoteDataMapping incidentMapping = InternalFunctions.FindMappingByExternalKey(projectId, externalBugId, incidentMappings, false);

                int incidentId = -1;
                RemoteIncident remoteIncident = null;
                if (incidentMapping == null)
                {
                    //This bug needs to be inserted into SpiraTest
                    remoteIncident = new RemoteIncident();
                    remoteIncident.ProjectId = projectId;

                    //Set the name for new incidents
                    if (String.IsNullOrEmpty(externalBugName))
                    {
                        remoteIncident.Name = "Name Not Specified";
                    }
                    else
                    {
                        remoteIncident.Name = externalBugName;
                    }

                    //Set the description for new incidents
                    if (String.IsNullOrEmpty(externalBugDescription))
                    {
                        remoteIncident.Description = "Description Not Specified";
                    }
                    else
                    {
                        remoteIncident.Description = externalBugDescription;
                    }

                    //Set the creation date for new incidents, if specified in redmine
                    if (redmineIssue.CreatedOn.HasValue)
                    {
                        remoteIncident.CreationDate = redmineIssue.CreatedOn.Value.ToUniversalTime();
                    }

                    //Set the dectector for new incidents
                    if (!String.IsNullOrEmpty(externalBugCreator))
                    {
                        RemoteDataMapping dataMapping = FindUserMappingByExternalKey(externalBugCreator, userMappings, spiraImportExport, redmineManager);
                        if (dataMapping == null)
                        {
                            //We can't find the matching user so log and ignore
                            LogErrorEvent("Unable to locate mapping entry for " + EXTERNAL_SYSTEM_NAME + " user " + externalBugCreator + " so using synchronization user as detector.", EventLogEntryType.Warning);
                        }
                        else
                        {
                            remoteIncident.OpenerId = dataMapping.InternalId;
                            LogTraceEvent(eventLog, "Got the detector " + remoteIncident.OpenerId.ToString() + "\n", EventLogEntryType.Information);
                        }
                    }
                }
                else
                {
                    //We need to load the matching SpiraTest incident and update
                    incidentId = incidentMapping.InternalId;

                    //Now retrieve the SpiraTest incident using the Import APIs
                    try
                    {
                        remoteIncident = spiraImportExport.Incident_RetrieveById(incidentId);


                        //Update the name for existing incidents
                        if (!String.IsNullOrEmpty(externalBugName))
                        {
                            remoteIncident.Description = externalBugName;
                        }

                        //Update the description for existing incidents
                        if (!String.IsNullOrEmpty(externalBugDescription))
                        {
                            remoteIncident.Description = externalBugDescription;
                        }
                    }
                    catch (Exception)
                    {
                        //Ignore as it will leave the remoteIncident as null
                    }
                }

                try
                {
                    //Make sure we have retrieved or created the incident
                    if (remoteIncident != null)
                    {
                        RemoteDataMapping dataMapping;
                        LogTraceEvent(eventLog, "Retrieved incident in " + productName + "\n", EventLogEntryType.Information);

                        //Now get the bug priority from the mapping (if priority is set)
                        if (String.IsNullOrEmpty(externalBugPriority))
                        {
                            remoteIncident.PriorityId = null;
                        }
                        else
                        {
                            dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, externalBugPriority, priorityMappings, true);
                            if (dataMapping == null)
                            {
                                //We can't find the matching item so log and just don't set the priority
                                LogErrorEvent("Unable to locate mapping entry for " + EXTERNAL_SYSTEM_NAME + " bug priority " + externalBugPriority + " in project " + projectId, EventLogEntryType.Warning);
                            }
                            else
                            {
                                remoteIncident.PriorityId = dataMapping.InternalId;
                            }
                        }
                        LogTraceEvent(eventLog, "Got the priority\n", EventLogEntryType.Information);

                        //Now get the bug severity from the mapping (if severity is set)
                        if (String.IsNullOrEmpty(externalBugSeverity))
                        {
                            remoteIncident.SeverityId = null;
                        }
                        else
                        {
                            dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, externalBugSeverity, severityMappings, true);
                            if (dataMapping == null)
                            {
                                //We can't find the matching item so log and just don't set the severity
                                LogErrorEvent("Unable to locate mapping entry for " + EXTERNAL_SYSTEM_NAME + " bug severity " + externalBugSeverity + " in project " + projectId, EventLogEntryType.Warning);
                            }
                            else
                            {
                                remoteIncident.SeverityId = dataMapping.InternalId;
                            }
                        }
                        LogTraceEvent(eventLog, "Got the severity\n", EventLogEntryType.Information);

                        //Now get the bug status from the mapping
                        if (!String.IsNullOrEmpty(externalBugStatus))
                        {
                            dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, externalBugStatus, statusMappings, true);
                            if (dataMapping == null)
                            {
                                //We can't find the matching item so log and ignore
                                LogErrorEvent("Unable to locate mapping entry for " + EXTERNAL_SYSTEM_NAME + " bug status " + externalBugStatus + " in project " + projectId, EventLogEntryType.Error);
                            }
                            else
                            {
                                remoteIncident.IncidentStatusId = dataMapping.InternalId;
                            }
                        }

                        LogTraceEvent(eventLog, "Got the status\n", EventLogEntryType.Information);

                        //Now get the bug type from the mapping
                        if (!String.IsNullOrEmpty(externalBugType))
                        {
                            dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, externalBugType, typeMappings, true);
                            if (dataMapping == null)
                            {
                                //If this is a new issue and we don't have the type mapped
                                //it means that they don't want them getting added to SpiraTest
                                if (incidentId == -1)
                                {
                                    return;
                                }
                                //We can't find the matching item so log and ignore
                                eventLog.WriteEntry("Unable to locate mapping entry for " + EXTERNAL_SYSTEM_NAME + " incident type " + externalBugType + " in project " + projectId, EventLogEntryType.Error);
                            }
                            else
                            {
                                remoteIncident.IncidentTypeId = dataMapping.InternalId;
                            }
                        }
                        LogTraceEvent(eventLog, "Got the type\n", EventLogEntryType.Information);

                        //Now update the bug's owner/assignee in SpiraTest
                        if (!String.IsNullOrEmpty(externalBugAssignee))
                        {
                            dataMapping = FindUserMappingByExternalKey(externalBugAssignee, userMappings, spiraImportExport, redmineManager);
                            if (dataMapping == null)
                            {
                                //We can't find the matching user so log and ignore
                                LogErrorEvent("Unable to locate mapping entry for " + EXTERNAL_SYSTEM_NAME + " user " + externalBugAssignee + " so ignoring the assignee change", EventLogEntryType.Warning);
                            }
                            else
                            {
                                remoteIncident.OwnerId = dataMapping.InternalId;
                                LogTraceEvent(eventLog, "Got the assignee " + remoteIncident.OwnerId.ToString() + "\n", EventLogEntryType.Information);
                            }
                        }

                        //Update the start-date if necessary
                        if (externalBugStartDate.HasValue)
                        {
                            remoteIncident.StartDate = externalBugStartDate.Value.ToUniversalTime();
                        }

                        //Update the closed-date if necessary
                        if (externalBugClosedDate.HasValue)
                        {
                            remoteIncident.ClosedDate = externalBugClosedDate.Value.ToUniversalTime();
                        }

                        //Update the effort values if provided
                        if (externalEstimatedEffortInMinutes.HasValue)
                        {
                            remoteIncident.EstimatedEffort = externalEstimatedEffortInMinutes.Value;
                        }
                        //if (externalActualEffortInMinutes.HasValue)
                        //{
                        //    remoteIncident.ActualEffort = externalActualEffortInMinutes.Value;
                        //}
                        if (externalRemainingEffortInMinutes.HasValue)
                        {
                            remoteIncident.RemainingEffort = externalRemainingEffortInMinutes.Value;
                        }

                        //Now get the list of comments attached to the SpiraTest incident
                        //If this is the new incident case, just leave as null
                        RemoteComment[] incidentComments = null;
                        if (incidentId != -1)
                        {
                            incidentComments = spiraImportExport.Incident_RetrieveComments(incidentId);
                        }

                        //Iterate through all the comments and see if we need to add any to SpiraTest
                        List<RemoteComment> newIncidentComments = new List<RemoteComment>();

                        //Now we need to get all the comments attached to the bug in the external system
                        if (redmineIssue.Journals != null && redmineIssue.Journals.Count > 0)
                        {
                            LogTraceEvent(eventLog, "Found " + redmineIssue.Journals.Count + " issue journal entries in " + EXTERNAL_SYSTEM_NAME, EventLogEntryType.Information);

                            foreach (Journal externalBugComment in redmineIssue.Journals)
                            {
                                //Extract the resolution values from the external system
                                string externalCommentText = externalBugComment.Notes;
                                if (!String.IsNullOrWhiteSpace(externalCommentText))
                                {
                                    string externalCommentCreator = externalBugComment.User.Id.ToString();
                                    DateTime? externalCommentCreationDate = externalBugComment.CreatedOn;

                                    //See if we already have this resolution inside SpiraTest
                                    bool alreadyAdded = false;
                                    if (incidentComments != null)
                                    {
                                        foreach (RemoteComment incidentComment in incidentComments)
                                        {
                                            if (incidentComment.Text.Trim() == externalCommentText.Trim())
                                            {
                                                alreadyAdded = true;
                                            }
                                        }
                                    }
                                    if (!alreadyAdded)
                                    {
                                        //Get the resolution author mapping
                                        LogTraceEvent(eventLog, "Looking for " + EXTERNAL_SYSTEM_NAME + " comments creator: '" + externalCommentCreator + "'\n", EventLogEntryType.Information);
                                        dataMapping = InternalFunctions.FindMappingByExternalKey(externalCommentCreator, userMappings);
                                        int? creatorId = null;
                                        if (dataMapping != null)
                                        {
                                            //Set the creator of the comment, otherwise leave null and SpiraTest will
                                            //simply use the synchronization user
                                            creatorId = dataMapping.InternalId;
                                        }

                                        //Add the comment to SpiraTest
                                        RemoteComment newIncidentComment = new RemoteComment();
                                        newIncidentComment.ArtifactId = incidentId;
                                        newIncidentComment.UserId = creatorId;
                                        newIncidentComment.CreationDate = (externalCommentCreationDate.HasValue) ? externalCommentCreationDate.Value.ToUniversalTime() : DateTime.UtcNow;
                                        newIncidentComment.Text = externalCommentText;
                                        newIncidentComments.Add(newIncidentComment);
                                    }
                                }
                            }

                            //The resolutions will actually get added later when we insert/update the incident record itself
                            LogTraceEvent(eventLog, "Got the comments/resolution\n", EventLogEntryType.Information);
                        }
                        else
                        {
                            LogTraceEvent(eventLog, "No comments/resolutions associated with issue " + externalBugId, EventLogEntryType.Information);
                        }

                        //Specify the detected-in release if applicable
                        //if (!String.IsNullOrEmpty(externalBugDetectedRelease))
                        //{
                        //    //See if we have a mapped SpiraTest release in either the existing list of
                        //    //mapped releases or the list of newly added ones
                        //    dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, externalBugDetectedRelease, releaseMappings, false);
                        //    if (dataMapping == null)
                        //    {
                        //        dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, externalBugDetectedRelease, newReleaseMappings.ToArray(), false);
                        //    }
                        //    if (dataMapping == null)
                        //    {
                        //        //We can't find the matching item so need to create a new release in SpiraTest and add to mappings

                        //        /*
                        //         * Add code to retrieve the release/version in the external system (if necessary) and extract the properties
                        //         *       into the following temporary variables
                        //         */
                        //        string externalReleaseName = "";
                        //        string externalReleaseVersionNumber = "";
                        //        DateTime? externalReleaseStartDate = null;
                        //        DateTime? externalReleaseEndDate = null;

                        //        LogTraceEvent(eventLog, "Adding new release in " + productName + " for version " + externalBugDetectedRelease + "\n", EventLogEntryType.Information);
                        //        RemoteRelease remoteRelease = new RemoteRelease();
                        //        remoteRelease.Name = externalReleaseName;
                        //        if (externalReleaseVersionNumber.Length > 10)
                        //        {
                        //            remoteRelease.VersionNumber = externalReleaseVersionNumber.Substring(0, 10);
                        //        }
                        //        else
                        //        {
                        //            remoteRelease.VersionNumber = externalReleaseVersionNumber;
                        //        }
                        //        remoteRelease.Active = true;
                        //        //If no start-date specified, simply use now
                        //        remoteRelease.StartDate = (externalReleaseStartDate.HasValue) ? externalReleaseStartDate.Value : DateTime.Now;
                        //        //If no end-date specified, simply use 1-month from now
                        //        remoteRelease.EndDate = (externalReleaseEndDate.HasValue) ? externalReleaseEndDate.Value : DateTime.Now.AddMonths(1);
                        //        remoteRelease.CreatorId = remoteIncident.OpenerId;
                        //        remoteRelease.CreationDate = DateTime.Now;
                        //        remoteRelease.ResourceCount = 1;
                        //        remoteRelease.DaysNonWorking = 0;
                        //        remoteRelease = spiraImportExport.Release_Create(remoteRelease, null);

                        //        //Add a new mapping entry
                        //        RemoteDataMapping newReleaseMapping = new RemoteDataMapping();
                        //        newReleaseMapping.ProjectId = projectId;
                        //        newReleaseMapping.InternalId = remoteRelease.ReleaseId.Value;
                        //        newReleaseMapping.ExternalKey = externalBugDetectedRelease;
                        //        newReleaseMappings.Add(newReleaseMapping);
                        //        remoteIncident.DetectedReleaseId = newReleaseMapping.InternalId;
                        //        LogTraceEvent(eventLog, "Setting detected release id to  " + newReleaseMapping.InternalId + "\n", EventLogEntryType.SuccessAudit);
                        //    }
                        //    else
                        //    {
                        //        remoteIncident.DetectedReleaseId = dataMapping.InternalId;
                        //        LogTraceEvent(eventLog, "Setting detected release id to  " + dataMapping.InternalId + "\n", EventLogEntryType.SuccessAudit);
                        //    }
                        //}

                        //Specify the resolved-in release if applicable
                        if (!String.IsNullOrEmpty(externalBugResolvedRelease))
                        {
                            //See if we have a mapped SpiraTest release in either the existing list of
                            //mapped releases or the list of newly added ones
                            dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, externalBugResolvedRelease, releaseMappings, false);
                            if (dataMapping == null)
                            {
                                dataMapping = InternalFunctions.FindMappingByExternalKey(projectId, externalBugResolvedRelease, newReleaseMappings.ToArray(), false);
                            }
                            if (dataMapping == null)
                            {
                                //We can't find the matching item so need to create a new release in SpiraTest and add to mappings
                                LogTraceEvent(eventLog, "Adding new release in " + productName + " for version " + externalBugResolvedRelease + "\n", EventLogEntryType.Information);
                                RemoteRelease remoteRelease = new RemoteRelease();
                                remoteRelease.Name = externalBugResolvedReleaseName;
                                if (externalBugResolvedReleaseName.Length > 10)
                                {
                                    remoteRelease.VersionNumber = externalBugResolvedReleaseName.Substring(0, 10);
                                }
                                else
                                {
                                    remoteRelease.VersionNumber = externalBugResolvedReleaseName;
                                }
                                remoteRelease.Active = true;
                                //If no start-date specified, simply use now
                                remoteRelease.StartDate = DateTime.UtcNow;
                                //If no end-date specified, simply use 1-month from now
                                remoteRelease.EndDate = DateTime.UtcNow.AddMonths(1);
                                remoteRelease.CreatorId = remoteIncident.OpenerId;
                                remoteRelease.CreationDate = DateTime.UtcNow;
                                remoteRelease.ResourceCount = 1;
                                remoteRelease.DaysNonWorking = 0;
                                remoteRelease = spiraImportExport.Release_Create(remoteRelease, null);

                                //Add a new mapping entry
                                RemoteDataMapping newReleaseMapping = new RemoteDataMapping();
                                newReleaseMapping.ProjectId = projectId;
                                newReleaseMapping.InternalId = remoteRelease.ReleaseId.Value;
                                newReleaseMapping.ExternalKey = externalBugResolvedRelease;
                                newReleaseMappings.Add(newReleaseMapping);
                                remoteIncident.ResolvedReleaseId = newReleaseMapping.InternalId;
                                LogTraceEvent(eventLog, "Setting resolved release id to  " + newReleaseMapping.InternalId + "\n", EventLogEntryType.SuccessAudit);
                            }
                            else
                            {
                                remoteIncident.ResolvedReleaseId = dataMapping.InternalId;
                                LogTraceEvent(eventLog, "Setting resolved release id to  " + dataMapping.InternalId + "\n", EventLogEntryType.SuccessAudit);
                            }
                        }

                        //Need to get the list of custom property values for the bug from the external system.
                        IList<CustomField> externalSystemCustomFieldValues = redmineIssue.CustomFields;
                        
                        //Now we need to see if any of the custom fields have changed in the external system bug
                        if (externalSystemCustomFieldValues != null)
                        {
                            ProcessExternalSystemCustomFields(productName, projectId, remoteIncident, externalSystemCustomFieldValues, incidentCustomProperties, customPropertyMappingList, customPropertyValueMappingList, userMappings, spiraImportExport);
                        }

                        //Finally add or update the incident in SpiraTest
                        if (incidentId == -1)
                        {
                            if (this.createNewItemsInSpira)
                            {
                                //Debug logging - comment out for production code
                                try
                                {
                                    remoteIncident = spiraImportExport.Incident_Create(remoteIncident);
                                }
                                catch (Exception exception)
                                {
                                    LogErrorEvent("Error Adding " + EXTERNAL_SYSTEM_NAME + " bug " + externalBugId + " to " + productName + " (" + exception.Message + ")\n" + exception.StackTrace, EventLogEntryType.Error);
                                    return;
                                }
                                LogTraceEvent(eventLog, "Successfully added " + EXTERNAL_SYSTEM_NAME + " bug " + externalBugId + " to " + productName + "\n", EventLogEntryType.Information);

                                //Extract the SpiraTest incident and add to mappings table
                                RemoteDataMapping newIncidentMapping = new RemoteDataMapping();
                                newIncidentMapping.ProjectId = projectId;
                                newIncidentMapping.InternalId = remoteIncident.IncidentId.Value;
                                newIncidentMapping.ExternalKey = externalBugId;
                                newIncidentMappings.Add(newIncidentMapping);

                                //Now add any comments (need to set the ID)
                                foreach (RemoteComment newIncidentComment in newIncidentComments)
                                {
                                    newIncidentComment.ArtifactId = remoteIncident.IncidentId.Value;
                                }
                                spiraImportExport.Incident_AddComments(newIncidentComments.ToArray());

                                /*
                                * Need to add the base URL onto the URL that we use to link the Spira incident to the external system
                                */
                                if (!String.IsNullOrEmpty(EXTERNAL_BUG_URL))
                                {
                                    try
                                    {
                                        string externalUrl = connectionString + String.Format(EXTERNAL_BUG_URL, externalBugId);
                                        RemoteDocument remoteUrl = new RemoteDocument();
                                        remoteUrl.ArtifactId = remoteIncident.IncidentId.Value;
                                        remoteUrl.ArtifactTypeId = (int)Constants.ArtifactType.Incident;
                                        remoteUrl.Description = "Link to issue in " + EXTERNAL_SYSTEM_NAME;
                                        remoteUrl.FilenameOrUrl = externalUrl;
                                        spiraImportExport.Document_AddUrl(remoteUrl);
                                    }
                                    catch (Exception exception)
                                    {
                                        //Log a message that describes why it's not working
                                        LogErrorEvent("Unable to add " + EXTERNAL_SYSTEM_NAME + " hyperlink to the " + productName + " incident, error was: " + exception.Message + "\n" + exception.StackTrace, EventLogEntryType.Warning);
                                        //Just continue with the rest since it's optional.
                                    }
                                }

                                //Get any file attachments associated with the issue
                                if (redmineIssue.Attachments != null)
                                {
                                    foreach (Attachment attachment in redmineIssue.Attachments)
                                    {
                                        try
                                        {
                                            string downloadUrl = attachment.ContentUrl;
                                            RedmineWebClient webClient = new RedmineWebClient();
                                            webClient.QueryString["key"] = this.externalLogin;
                                            webClient.UseDefaultCredentials = false;
                                            webClient.Headers.Add("Content-Type", "application/octet-stream");
                                            // Workaround - it seems that WebClient doesn't send credentials in each POST request
                                            string basicAuthorization = "Basic " + Convert.ToBase64String(Encoding.ASCII.GetBytes(this.externalLogin + ":" + this.externalPassword));
                                            webClient.Headers.Add("Authorization", basicAuthorization);

                                            byte[] binaryData = webClient.DownloadData(downloadUrl);
                                            RemoteDocument remoteDocument = new RemoteDocument();
                                            remoteDocument.FilenameOrUrl = attachment.FileName;
                                            remoteDocument.ArtifactId = remoteIncident.IncidentId.Value;
                                            remoteDocument.ArtifactTypeId = (int)Constants.ArtifactType.Incident;
                                            remoteDocument.Description = attachment.Description;
                                            remoteDocument.UploadDate = (attachment.CreatedOn.HasValue) ? attachment.CreatedOn.Value : DateTime.UtcNow;
                                            spiraImportExport.Document_AddFile(remoteDocument, binaryData);
                                        }
                                        catch (Exception exception)
                                        {
                                            //Log message and carry on
                                            LogErrorEvent(String.Format("Unable to attach {0} issue attachment '{1}' to {2} incident. Error: {3}", EXTERNAL_SYSTEM_NAME, attachment.FileName, productName, exception.Message), EventLogEntryType.Error);
                                        }
                                    }
                                }

                                //Add any incident-to-incident associations
                                //We need to get the destination incident id from the external target bug id from data mapping
                                if (redmineIssue.Relations != null)
                                {
                                    foreach (IssueRelation relation in redmineIssue.Relations)
                                    {
                                        string externalTargetBugId = relation.IssueToId.ToString();    // Replace with real code to get the ID of the target bug in the external system
                                        dataMapping = InternalFunctions.FindMappingByExternalKey(externalTargetBugId, incidentMappings);
                                        if (dataMapping != null)
                                        {
                                            //Create the new incident association
                                            RemoteAssociation remoteAssociation = new RemoteAssociation();
                                            remoteAssociation.DestArtifactId = dataMapping.InternalId;
                                            remoteAssociation.DestArtifactTypeId = (int)Constants.ArtifactType.Incident;
                                            remoteAssociation.CreationDate = DateTime.UtcNow;
                                            remoteAssociation.Comment = relation.Type.ToString();
                                            remoteAssociation.SourceArtifactId = remoteIncident.IncidentId.Value;
                                            remoteAssociation.SourceArtifactTypeId = (int)Constants.ArtifactType.Incident;
                                            spiraImportExport.Association_Create(remoteAssociation);
                                        }
                                    }
                                }
                                else
                                {
                                    LogTraceEvent(eventLog, "No relations associated with issue " + externalBugId, EventLogEntryType.Information);
                                }
                            }
                        }
                        else
                        {
                            spiraImportExport.Incident_Update(remoteIncident);

                            //Now add any resolutions
                            spiraImportExport.Incident_AddComments(newIncidentComments.ToArray());

                            //Debug logging - comment out for production code
                            LogTraceEvent(eventLog, "Successfully updated\n", EventLogEntryType.Information);
                        }
                    }
                }
                catch (FaultException<ValidationFaultMessage> validationException)
                {
                    string message = "";
                    ValidationFaultMessage validationFaultMessage = validationException.Detail;
                    message = validationFaultMessage.Summary + ": \n";
                    {
                        foreach (ValidationFaultMessageItem messageItem in validationFaultMessage.Messages)
                        {
                            message += messageItem.FieldName + "=" + messageItem.Message + " \n";
                        }
                    }
                    LogErrorEvent("Error Inserting/Updating " + EXTERNAL_SYSTEM_NAME + " Bug " + externalBugId + " in " + productName + " (" + message + ")\n" + validationException.StackTrace, EventLogEntryType.Error);
                }
                catch (Exception exception)
                {
                    //Log and continue execution
                    LogErrorEvent("Error Inserting/Updating " + EXTERNAL_SYSTEM_NAME + " Bug " + externalBugId + " in " + productName + ": " + exception.Message + "\n" + exception.StackTrace, EventLogEntryType.Error);
                }
            }
        }

        /// <summary>
        /// Updates the Spira incident object's custom properties with any new/changed custom fields from the external bug object
        /// </summary>
        /// <param name="projectId">The id of the current project</param>
        /// <param name="spiraImportExport">The Spira API proxy class</param>
        /// <param name="remoteArtifact">The Spira artifact</param>
        /// <param name="customPropertyMappingList">The mapping of custom properties</param>
        /// <param name="customPropertyValueMappingList">The mapping of custom property list values</param>
        /// <param name="externalSystemCustomFieldValues">The list of custom fields in the external system</param>
        /// <param name="productName">The name of the product being connected to (SpiraTest, SpiraPlan, etc.)</param>
        /// <param name="userMappings">The user mappings</param>
        private void ProcessExternalSystemCustomFields(string productName, int projectId, RemoteArtifact remoteArtifact, IList<CustomField> externalSystemCustomFieldValues, RemoteCustomProperty[] customProperties, Dictionary<int, RemoteDataMapping> customPropertyMappingList, Dictionary<int, RemoteDataMapping[]> customPropertyValueMappingList, RemoteDataMapping[] userMappings, ImportExportClient spiraImportExport)
        {
            //Loop through all the defined Spira custom properties
            foreach (RemoteCustomProperty customProperty in customProperties)
            {
                //Get the external key of this custom property (if it has one)
                if (customPropertyMappingList.ContainsKey(customProperty.CustomPropertyId.Value))
                {
                    RemoteDataMapping customPropertyDataMapping = customPropertyMappingList[customProperty.CustomPropertyId.Value];
                    if (customPropertyDataMapping != null)
                    {
                        LogTraceEvent(eventLog, "Found custom property mapping for " + EXTERNAL_SYSTEM_NAME + " field " + customPropertyDataMapping.ExternalKey + "\n", EventLogEntryType.Information);
                        string externalKey = customPropertyDataMapping.ExternalKey;
                        //See if we have a list, multi-list or user custom field as they need to be handled differently
                        if (customProperty.CustomPropertyTypeId == (int)Constants.CustomPropertyType.List)
                        {
                            LogTraceEvent(eventLog, EXTERNAL_SYSTEM_NAME + " field " + customPropertyDataMapping.ExternalKey + " is mapped to a LIST property\n", EventLogEntryType.Information);

                            //Now we need to set the value on the SpiraTest incident
                            CustomField cf = externalSystemCustomFieldValues.FirstOrDefault(f => f.Id.ToString() == externalKey);
                            if (cf != null)
                            {
                                if (cf.Values == null || cf.Values.Count < 1)
                                {
                                    InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, (int?)null);
                                }
                                else
                                {
                                    //Need to get the Spira custom property value
                                    string fieldValue = cf.Values[0].Info;
                                    RemoteDataMapping[] customPropertyValueMappings = customPropertyValueMappingList[customProperty.CustomPropertyId.Value];
                                    RemoteDataMapping customPropertyValueMapping = InternalFunctions.FindMappingByExternalKey(projectId, fieldValue, customPropertyValueMappings, false);
                                    if (customPropertyValueMapping != null)
                                    {
                                        InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, customPropertyValueMapping.InternalId);
                                    }
                                }
                            }
                            else
                            {
                                LogErrorEvent(String.Format("" + EXTERNAL_SYSTEM_NAME + " bug doesn't have a field definition for '{0}'\n", externalKey), EventLogEntryType.Warning);
                            }
                        }
                        /* USER CUSTOM PROPERTIES are not currently supported
                        else if (customProperty.CustomPropertyTypeId == (int)Constants.CustomPropertyType.User)
                        {
                            LogTraceEvent(eventLog, EXTERNAL_SYSTEM_NAME + " field " + customPropertyDataMapping.ExternalKey + " is mapped to a USER property\n", EventLogEntryType.Information);

                            //Now we need to set the value on the SpiraTest incident
                            if (externalSystemCustomFieldValues.ContainsKey(externalKey))
                            {
                                if (externalSystemCustomFieldValues[externalKey] == null)
                                {
                                    InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, (int?)null);
                                }
                                else
                                {
                                    //Need to get the Spira custom property value
                                    string fieldValue = externalSystemCustomFieldValues[externalKey].ToString();
                                    RemoteDataMapping customPropertyValueMapping = FindUserMappingByExternalKey(fieldValue, userMappings, spiraImportExport);
                                    if (customPropertyValueMapping != null)
                                    {
                                        InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, customPropertyValueMapping.InternalId);
                                    }
                                }
                            }
                            else
                            {
                                LogErrorEvent(String.Format("" + EXTERNAL_SYSTEM_NAME + " bug doesn't have a field definition for '{0}'\n", externalKey), EventLogEntryType.Warning);
                            }
                        }*/
                        else if (customProperty.CustomPropertyTypeId == (int)Constants.CustomPropertyType.MultiList)
                        {
                            LogTraceEvent(eventLog, EXTERNAL_SYSTEM_NAME + " field " + customPropertyDataMapping.ExternalKey + " is mapped to a MULTILIST property\n", EventLogEntryType.Information);

                            //Next the multi-list fields
                            //Now we need to set the value on the SpiraTest incident
                            CustomField cf = externalSystemCustomFieldValues.FirstOrDefault(f => f.Id.ToString() == externalKey);
                            if (cf != null)
                            {
                                if (cf.Values == null || cf.Values.Count < 1)
                                {
                                    InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, (List<int>)null);
                                }
                                else
                                {
                                    //Need to get the Spira custom property values
                                    RemoteDataMapping[] customPropertyValueMappings = customPropertyValueMappingList[customProperty.CustomPropertyId.Value];

                                    //Data-map each of the custom property values
                                    //We assume that the external system has a multiselect stored list of string values (List<string>)
                                    List<int> spiraCustomValueIds = new List<int>();
                                    foreach (CustomFieldValue cfv in cf.Values)
                                    {
                                        RemoteDataMapping customPropertyValueMapping = InternalFunctions.FindMappingByExternalKey(projectId, cfv.Info, customPropertyValueMappings, false);
                                        if (customPropertyValueMapping != null)
                                        {
                                            spiraCustomValueIds.Add(customPropertyValueMapping.InternalId);
                                        }
                                    }
                                    InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, spiraCustomValueIds);
                                }
                            }
                            else
                            {
                                LogErrorEvent(String.Format("" + EXTERNAL_SYSTEM_NAME + " bug doesn't have a field definition for '{0}'\n", externalKey), EventLogEntryType.Warning);
                            }
                        }
                        else
                        {
                            LogTraceEvent(eventLog, EXTERNAL_SYSTEM_NAME + " field " + customPropertyDataMapping.ExternalKey + " is mapped to a VALUE property\n", EventLogEntryType.Information);

                            //Now we need to set the value on the SpiraTest artifact
                            CustomField cf = externalSystemCustomFieldValues.FirstOrDefault(f => f.Id.ToString() == externalKey);
                            if (cf != null)
                            {
                                switch ((Constants.CustomPropertyType)customProperty.CustomPropertyTypeId)
                                {
                                    case Constants.CustomPropertyType.Boolean:
                                        {
                                            if (cf.Values == null || cf.Values.Count < 1)
                                            {
                                                InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, (bool?)null);
                                            }
                                            else
                                            {
                                                bool fieldValue;
                                                if (Boolean.TryParse(cf.Values[0].Info, out fieldValue))
                                                {
                                                    InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, fieldValue);
                                                    LogTraceEvent(eventLog, "Setting " + EXTERNAL_SYSTEM_NAME + " field " + customPropertyDataMapping.ExternalKey + " value '" + fieldValue + "' on artifact\n", EventLogEntryType.Information);
                                                }
                                            }
                                        }
                                        break;

                                    case Constants.CustomPropertyType.Date:
                                        {
                                            if (cf.Values == null || cf.Values.Count < 1)
                                            {
                                                InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, (DateTime?)null);
                                            }
                                            else
                                            {
                                                DateTime fieldValue;
                                                if (DateTime.TryParse(cf.Values[0].Info, out fieldValue))
                                                {
                                                    //Need to convert to UTC for Spira
                                                    DateTime localTime = fieldValue;
                                                    DateTime utcTime = localTime.ToUniversalTime();

                                                    InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, utcTime);
                                                    LogTraceEvent(eventLog, "Setting " + EXTERNAL_SYSTEM_NAME + " field " + customPropertyDataMapping.ExternalKey + " value '" + utcTime + "' on artifact\n", EventLogEntryType.Information);
                                                }
                                            }
                                        }
                                        break;


                                    case Constants.CustomPropertyType.Decimal:
                                        {
                                            if (cf.Values == null || cf.Values.Count < 1)
                                            {
                                                InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, (decimal?)null);
                                            }
                                            else
                                            {
                                                decimal fieldValue;
                                                if (Decimal.TryParse(cf.Values[0].Info, out fieldValue))
                                                {
                                                    InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, fieldValue);
                                                    LogTraceEvent(eventLog, "Setting " + EXTERNAL_SYSTEM_NAME + " field " + customPropertyDataMapping.ExternalKey + " value '" + fieldValue + "' on artifact\n", EventLogEntryType.Information);
                                                }
                                            }
                                        }
                                        break;

                                    case Constants.CustomPropertyType.Integer:
                                        {
                                            if (cf.Values == null || cf.Values.Count < 1)
                                            {
                                                InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, (int?)null);
                                            }
                                            else
                                            {
                                                int fieldValue;
                                                if (Int32.TryParse(cf.Values[0].Info, out fieldValue))
                                                {
                                                    InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, fieldValue);
                                                    LogTraceEvent(eventLog, "Setting " + EXTERNAL_SYSTEM_NAME + " field " + customPropertyDataMapping.ExternalKey + " value '" + fieldValue + "' on artifact\n", EventLogEntryType.Information);
                                                }
                                            }
                                        }
                                        break;

                                    case Constants.CustomPropertyType.Text:
                                    default:
                                        {
                                            if (cf.Values == null || cf.Values.Count < 1)
                                            {
                                                InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, (string)null);
                                            }
                                            else
                                            {
                                                string fieldValue = cf.Values[0].Info;
                                                InternalFunctions.SetCustomPropertyValue(remoteArtifact, customProperty.PropertyNumber, fieldValue);
                                                LogTraceEvent(eventLog, "Setting " + EXTERNAL_SYSTEM_NAME + " field " + customPropertyDataMapping.ExternalKey + " value '" + fieldValue + "' on artifact\n", EventLogEntryType.Information);
                                            }
                                        }
                                        break;
                                }
                            }
                            else
                            {
                                LogErrorEvent(String.Format("" + EXTERNAL_SYSTEM_NAME + " bug doesn't have a field definition for '{0}'\n", externalKey), EventLogEntryType.Warning);
                            }
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Updates the external bug object with any incident custom property values
        /// </summary>
        /// <param name="projectId">The id of the current project</param>
        /// <param name="spiraImportExport">The Spira API proxy class</param>
        /// <param name="remoteArtifact">The Spira artifact</param>
        /// <param name="customPropertyMappingList">The mapping of custom properties</param>
        /// <param name="customPropertyValueMappingList">The mapping of custom property list values</param>
        /// <param name="externalSystemCustomFieldValues">The list of custom fields in the external system</param>
        /// <param name="productName">The name of the product being connected to (SpiraTest, SpiraPlan, etc.)</param>
        /// <param name="userMappings">The user mappings</param>
        private void ProcessCustomProperties(string productName, int projectId, RemoteArtifact remoteArtifact, List<CustomField> externalSystemCustomFieldValues, Dictionary<int, RemoteDataMapping> customPropertyMappingList, Dictionary<int, RemoteDataMapping[]> customPropertyValueMappingList, RemoteDataMapping[] userMappings, ImportExportClient spiraImportExport)
        {
            if (remoteArtifact.CustomProperties != null)
            {
                foreach (RemoteArtifactCustomProperty artifactCustomProperty in remoteArtifact.CustomProperties)
                {
                    //Handle user, list and non-list separately since only the list types need to have value mappings
                    RemoteCustomProperty customProperty = artifactCustomProperty.Definition;
                    if (customProperty != null && customProperty.CustomPropertyId.HasValue)
                    {
                        if (customProperty.CustomPropertyTypeId == (int)Constants.CustomPropertyType.List)
                        {
                            //Single-Select List
                            LogTraceEvent(eventLog, "Checking list custom property: " + customProperty.Name + "\n", EventLogEntryType.Information);

                            //See if we have a custom property value set
                            //Get the corresponding external custom field (if there is one)
                            if (artifactCustomProperty.IntegerValue.HasValue && customPropertyMappingList != null && customPropertyMappingList.ContainsKey(customProperty.CustomPropertyId.Value))
                            {
                                LogTraceEvent(eventLog, "Got value for list custom property: " + customProperty.Name + " (" + artifactCustomProperty.IntegerValue.Value + ")\n", EventLogEntryType.Information);
                                RemoteDataMapping customPropertyDataMapping = customPropertyMappingList[customProperty.CustomPropertyId.Value];
                                if (customPropertyDataMapping != null)
                                {
                                    string externalCustomField = customPropertyDataMapping.ExternalKey;

                                    //Get the corresponding external custom field value (if there is one)
                                    if (!String.IsNullOrEmpty(externalCustomField) && customPropertyValueMappingList.ContainsKey(customProperty.CustomPropertyId.Value))
                                    {
                                        //Make sure the field is an integer
                                        int externalCustomFieldId;
                                        if (Int32.TryParse(externalCustomField, out externalCustomFieldId))
                                        {
                                            RemoteDataMapping[] customPropertyValueMappings = customPropertyValueMappingList[customProperty.CustomPropertyId.Value];
                                            if (customPropertyValueMappings != null)
                                            {
                                                RemoteDataMapping customPropertyValueMapping = InternalFunctions.FindMappingByInternalId(projectId, artifactCustomProperty.IntegerValue.Value, customPropertyValueMappings);
                                                if (customPropertyValueMapping != null)
                                                {
                                                    //Make sure we have a mapped custom field value in the external system
                                                    if (!String.IsNullOrEmpty(customPropertyValueMapping.ExternalKey))
                                                    {
                                                        string externalCustomFieldValue = customPropertyValueMapping.ExternalKey;

                                                        LogTraceEvent(eventLog, "The custom property corresponds to the " + EXTERNAL_SYSTEM_NAME + " '" + externalCustomField + "' field", EventLogEntryType.Information);
                                                        CustomField cf = new CustomField();
                                                        cf.Id = externalCustomFieldId;
                                                        CustomFieldValue cfv = new CustomFieldValue();
                                                        cfv.Info = externalCustomFieldValue;
                                                        cf.Values = new List<CustomFieldValue>();
                                                        cf.Values.Add(cfv);
                                                        externalSystemCustomFieldValues.Add(cf);
                                                    }
                                                }
                                            }
                                        }
                                        else
                                        {
                                            LogErrorEvent("The external key for " + productName + " custom property '" + customProperty.Name + "' needs to be an integer instead of '" + externalCustomField + "', so ignoring custom property value", EventLogEntryType.Warning);
                                        }
                                    }
                                }
                            }
                        }
                        else if (customProperty.CustomPropertyTypeId == (int)Constants.CustomPropertyType.MultiList)
                        {
                            //Multi-Select List
                            LogTraceEvent(eventLog, "Checking multi-list custom property: " + customProperty.Name + "\n", EventLogEntryType.Information);

                            //See if we have a custom property value set
                            //Get the corresponding external custom field (if there is one)
                            if (artifactCustomProperty.IntegerListValue != null && artifactCustomProperty.IntegerListValue.Length > 0 && customPropertyMappingList != null && customPropertyMappingList.ContainsKey(customProperty.CustomPropertyId.Value))
                            {
                                LogTraceEvent(eventLog, "Got values for multi-list custom property: " + customProperty.Name + " (Count=" + artifactCustomProperty.IntegerListValue.Length + ")\n", EventLogEntryType.Information);
                                RemoteDataMapping customPropertyDataMapping = customPropertyMappingList[customProperty.CustomPropertyId.Value];
                                if (customPropertyDataMapping != null && !String.IsNullOrEmpty(customPropertyDataMapping.ExternalKey))
                                {
                                    string externalCustomField = customPropertyDataMapping.ExternalKey;
                                    LogTraceEvent(eventLog, "Got external key for multi-list custom property: " + customProperty.Name + " = " + externalCustomField + "\n", EventLogEntryType.Information);

                                    //Make sure the field is an integer
                                    int externalCustomFieldId;
                                    if (Int32.TryParse(externalCustomField, out externalCustomFieldId))
                                    {
                                        //Loop through each value in the list
                                        List<string> externalCustomFieldValues = new List<string>();
                                        foreach (int customPropertyListValue in artifactCustomProperty.IntegerListValue)
                                        {
                                            //Get the corresponding external custom field value (if there is one)
                                            if (customPropertyValueMappingList.ContainsKey(customProperty.CustomPropertyId.Value))
                                            {
                                                RemoteDataMapping[] customPropertyValueMappings = customPropertyValueMappingList[customProperty.CustomPropertyId.Value];
                                                if (customPropertyValueMappings != null)
                                                {
                                                    RemoteDataMapping customPropertyValueMapping = InternalFunctions.FindMappingByInternalId(projectId, customPropertyListValue, customPropertyValueMappings);
                                                    if (customPropertyValueMapping != null)
                                                    {
                                                        LogTraceEvent(eventLog, "Added multi-list custom property field value: " + customProperty.Name + " (Value=" + customPropertyValueMapping.ExternalKey + ")\n", EventLogEntryType.Information);
                                                        externalCustomFieldValues.Add(customPropertyValueMapping.ExternalKey);
                                                    }
                                                }
                                            }
                                        }

                                        //Make sure that we have some values to set
                                        LogTraceEvent(eventLog, "Got mapped values for multi-list custom property: " + customProperty.Name + " (Count=" + externalCustomFieldValues.Count + ")\n", EventLogEntryType.Information);
                                        if (externalCustomFieldValues.Count > 0)
                                        {
                                            CustomField cf = new CustomField();
                                            cf.Id = externalCustomFieldId;
                                            cf.Values = new List<CustomFieldValue>();
                                            foreach (string externalCustomFieldValue in externalCustomFieldValues)
                                            {
                                                CustomFieldValue cfv = new CustomFieldValue();
                                                cfv.Info = externalCustomFieldValue;
                                                cf.Values.Add(cfv);
                                            }
                                            externalSystemCustomFieldValues.Add(cf);
                                        }
                                        else
                                        {
                                            CustomField cf = new CustomField();
                                            cf.Id = externalCustomFieldId;
                                            cf.Values = new List<CustomFieldValue>();
                                            externalSystemCustomFieldValues.Add(cf);
                                        }
                                    }
                                    else
                                    {
                                        LogErrorEvent("The external key for " + productName + " custom property '" + customProperty.Name + "' needs to be an integer instead of '" + externalCustomField + "', so ignoring custom property value", EventLogEntryType.Warning);
                                    }
                                }
                            }
                        }
                        /*                  WE DO NOT CURRENTLY SUPPORT SYNCING USER CUSTOM PROPERTIES
                         *                  else if (customProperty.CustomPropertyTypeId == (int)Constants.CustomPropertyType.User)
                                            {
                                                //User
                                                LogTraceEvent(eventLog, "Checking user custom property: " + customProperty.Name + "\n", EventLogEntryType.Information);

                                                //See if we have a custom property value set
                                                if (artifactCustomProperty.IntegerValue.HasValue)
                                                {
                                                    RemoteDataMapping customPropertyDataMapping = customPropertyMappingList[customProperty.CustomPropertyId.Value];
                                                    if (customPropertyDataMapping != null && !String.IsNullOrEmpty(customPropertyDataMapping.ExternalKey))
                                                    {
                                                        string externalCustomField = customPropertyDataMapping.ExternalKey;
                                                        LogTraceEvent(eventLog, "Got external key for user custom property: " + customProperty.Name + " = " + externalCustomField + "\n", EventLogEntryType.Information);

                                                        LogTraceEvent(eventLog, "Got value for user custom property: " + customProperty.Name + " (" + artifactCustomProperty.IntegerValue.Value + ")\n", EventLogEntryType.Information);
                                                        //Get the corresponding external system user (if there is one)
                                                        RemoteDataMapping dataMapping = FindUserMappingByInternalId(artifactCustomProperty.IntegerValue.Value, userMappings, spiraImportExport);
                                                        if (dataMapping != null)
                                                        {
                                                            string externalUserName = dataMapping.ExternalKey;
                                                            LogTraceEvent(eventLog, "Adding user custom property field value: " + customProperty.Name + " (Value=" + externalUserName + ")\n", EventLogEntryType.Information);
                                                            LogTraceEvent(eventLog, "The custom property corresponds to the " + EXTERNAL_SYSTEM_NAME + " '" + externalCustomField + "' field", EventLogEntryType.Information);
                                                            externalSystemCustomFieldValues[externalCustomField] = externalUserName;
                                                        }
                                                        else
                                                        {
                                                            LogErrorEvent("Unable to find a matching " + EXTERNAL_SYSTEM_NAME + " user for " + productName + " user with ID=" + artifactCustomProperty.IntegerValue.Value + " so leaving property null.", EventLogEntryType.Warning);
                                                        }
                                                    }
                                                }
                                            }*/
                        else
                        {
                            //Other
                            LogTraceEvent(eventLog, "Checking non-list custom property: " + customProperty.Name + "\n", EventLogEntryType.Information);

                            //See if we have a custom property value set
                            if (!String.IsNullOrEmpty(artifactCustomProperty.StringValue) || artifactCustomProperty.BooleanValue.HasValue
                                || artifactCustomProperty.DateTimeValue.HasValue || artifactCustomProperty.DecimalValue.HasValue
                                || artifactCustomProperty.IntegerValue.HasValue)
                            {
                                LogTraceEvent(eventLog, "Got value for non-list custom property: " + customProperty.Name + "\n", EventLogEntryType.Information);
                                //Get the corresponding external custom field (if there is one)
                                if (customPropertyMappingList != null && customPropertyMappingList.ContainsKey(customProperty.CustomPropertyId.Value))
                                {
                                    RemoteDataMapping customPropertyDataMapping = customPropertyMappingList[customProperty.CustomPropertyId.Value];
                                    if (customPropertyDataMapping != null)
                                    {
                                        string externalCustomField = customPropertyDataMapping.ExternalKey;

                                        //Make sure we have a mapped custom field in the external system mapped
                                        if (!String.IsNullOrEmpty(externalCustomField))
                                        {
                                            //Make sure the field is an integer
                                            int externalCustomFieldId;
                                            if (Int32.TryParse(externalCustomField, out externalCustomFieldId))
                                            {
                                                    LogTraceEvent(eventLog, "The custom property corresponds to the " + EXTERNAL_SYSTEM_NAME + " '" + externalCustomField + "' field", EventLogEntryType.Information);
                                                    object customFieldValue = InternalFunctions.GetCustomPropertyValue(artifactCustomProperty);
                                                    CustomField cf = new CustomField();
                                                    cf.Id = externalCustomFieldId;
                                                    CustomFieldValue cfv = new CustomFieldValue();
                                                    cfv.Info = customFieldValue.ToString();
                                                    cf.Values = new List<CustomFieldValue>() { cfv };
                                                    externalSystemCustomFieldValues.Add(cf);
                                                }
                                            }
                                            else
                                            {
                                                LogErrorEvent("The external key for " + productName + " custom property '" + customProperty.Name + "' needs to be an integer instead of '" + externalCustomField + "', so ignoring custom property value", EventLogEntryType.Warning);
                                            }
                                     }
                                }
                            }
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Finds a user mapping entry from the internal id
        /// </summary>
        /// <param name="internalId">The internal id</param>
        /// <param name="dataMappings">The list of mappings</param>
        /// <param name="client">The Spira web service client proxy</param>
        /// <param name="redmineManager">The Redmine web service client proxy</param>
        /// <returns>The matching entry or Null if none found</returns>
        /// <remarks>If we are auto-mapping users, it will lookup the user-id instead</remarks>
        protected RemoteDataMapping FindUserMappingByInternalId(int internalId, RemoteDataMapping[] dataMappings, ImportExportClient client, RedmineManager redmineManager)
        {
            if (this.autoMapUsers)
            {
                RemoteUser remoteUser = client.User_RetrieveById(internalId);
                if (remoteUser == null)
                {
                    return null;
                }
                string username = remoteUser.UserName;

                //Now we need to get the corresponding Redmine user ID from the login name
                IList<User> redmineUserMatches = redmineManager.GetUsers(UserStatus.STATUS_ACTIVE, username);
                if (redmineUserMatches != null && redmineUserMatches.Count > 0)
                {
                    RemoteDataMapping userMapping = new RemoteDataMapping();
                    userMapping.InternalId = remoteUser.UserId.Value;
                    userMapping.ExternalKey = redmineUserMatches[0].Id.ToString();
                    return userMapping;
                }

                //No matching Redmine user
                return null;
            }
            else
            {
                return InternalFunctions.FindMappingByInternalId(internalId, dataMappings);
            }
        }

        /// <summary>
        /// Finds a user mapping entry from the external key
        /// </summary>
        /// <param name="externalKey">The external key</param>
        /// <param name="dataMappings">The list of mappings</param>
        /// <param name="client">The Spira web service client proxy</param>
        /// <param name="redmineManager">The Redmine web service client proxy</param>
        /// <returns>The matching entry or Null if none found</returns>
        /// <remarks>If we are auto-mapping users, it will lookup the username instead</remarks>
        protected RemoteDataMapping FindUserMappingByExternalKey(string externalKey, RemoteDataMapping[] dataMappings, ImportExportClient client, RedmineManager redmineManager)
        {
            if (this.autoMapUsers)
            {
                try
                {
                    //The externalKey is actually the Redmine user ID not the login name, so need to get the redmine username first
                    User redmineUser = redmineManager.GetObject<User>(externalKey, null);

                    //Now get the corrsponding Spira user
                    RemoteUser remoteUser = client.User_RetrieveByUserName(redmineUser.Login);
                    if (remoteUser == null)
                    {
                        return null;
                    }
                    RemoteDataMapping userMapping = new RemoteDataMapping();
                    userMapping.InternalId = remoteUser.UserId.Value;
                    userMapping.ExternalKey = externalKey;
                    return userMapping;
                }
                catch (Exception)
                {
                    //User could not be found so return null
                    return null;
                }
            }
            else
            {
                return InternalFunctions.FindMappingByExternalKey(externalKey, dataMappings);
            }
        }

        /// <summary>
        /// Logs a trace event message if the configuration option is set
        /// </summary>
        /// <param name="eventLog">The event log handle</param>
        /// <param name="message">The message to log</param>
        /// <param name="type">The type of event</param>
        protected void LogTraceEvent(EventLog eventLog, string message, EventLogEntryType type = EventLogEntryType.Information)
        {
            if (traceLogging && eventLog != null)
            {
                if (message.Length > 31000)
                {
                    //Split into smaller lengths
                    int index = 0;
                    while (index < message.Length)
                    {
                        try
                        {
                            string messageElement = message.Substring(index, 31000);
                            this.eventLog.WriteEntry(messageElement, type);
                        }
                        catch (ArgumentOutOfRangeException)
                        {
                            string messageElement = message.Substring(index);
                            this.eventLog.WriteEntry(messageElement, type);
                        }
                        index += 31000;
                    }
                }
                else
                {
                    this.eventLog.WriteEntry(message, type);
                }
            }
        }

        /// <summary>
        /// Logs a trace event message if the configuration option is set
        /// </summary>
        /// <param name="message">The message to log</param>
        /// <param name="type">The type of event</param>
        public void LogErrorEvent(string message, EventLogEntryType type = EventLogEntryType.Error)
        {
            if (this.eventLog != null)
            {
                if (message.Length > 31000)
                {
                    //Split into smaller lengths
                    int index = 0;
                    while (index < message.Length)
                    {
                        try
                        {
                            string messageElement = message.Substring(index, 31000);
                            this.eventLog.WriteEntry(messageElement, type);
                        }
                        catch (ArgumentOutOfRangeException)
                        {
                            string messageElement = message.Substring(index);
                            this.eventLog.WriteEntry(messageElement, type);
                        }
                        index += 31000;
                    }
                }
                else
                {
                    this.eventLog.WriteEntry(message, type);
                }
            }
        }

        // Implement IDisposable.
        // Do not make this method virtual.
        // A derived class should not be able to override this method.
        public void Dispose()
        {
            Dispose(true);
            // Take yourself off the Finalization queue 
            // to prevent finalization code for this object
            // from executing a second time.
            GC.SuppressFinalize(this);
        }

        // Use C# destructor syntax for finalization code.
        // This destructor will run only if the Dispose method 
        // does not get called.
        // It gives your base class the opportunity to finalize.
        // Do not provide destructors in types derived from this class.
        ~DataSync()
        {
            // Do not re-create Dispose clean-up code here.
            // Calling Dispose(false) is optimal in terms of
            // readability and maintainability.
            Dispose(false);
        }

        // Dispose(bool disposing) executes in two distinct scenarios.
        // If disposing equals true, the method has been called directly
        // or indirectly by a user's code. Managed and unmanaged resources
        // can be disposed.
        // If disposing equals false, the method has been called by the 
        // runtime from inside the finalizer and you should not reference 
        // other objects. Only unmanaged resources can be disposed.
        protected virtual void Dispose(bool disposing)
        {
            // Check to see if Dispose has already been called.
            if (!this.disposed)
            {
                // If disposing equals true, dispose all managed 
                // and unmanaged resources.
                if (disposing)
                {
                    //Remove the event log reference
                    this.eventLog = null;
                }
                // Release unmanaged resources. If disposing is false, 
                // only the following code is executed.

                //This class doesn't have any unmanaged resources to worry about
            }
            disposed = true;
        }
    }
}
