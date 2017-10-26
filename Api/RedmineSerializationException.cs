using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Redmine.Net.Api
{
    /// <summary>
    /// Represents an exception that happens during deserialization of XML
    /// </summary>
    public class RedmineDeserializationException : Exception
    {
        protected string xml = "";

        public RedmineDeserializationException()
            : base() { }

        public RedmineDeserializationException(string message)
            : base(message) { }

        public RedmineDeserializationException(string message, Exception innerException)
            : base(message, innerException) { }

        public RedmineDeserializationException(string message, string xml, Exception innerException)
            : base(message, innerException)
        {
            this.xml = xml;
        }

        /// <summary>
        /// Returns the XML that could not be deserialized
        /// </summary>
        public string Xml
        {
            get
            {
                return this.xml;
            }
        }
    }
}
