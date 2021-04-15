using CsvImporter;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Text;

namespace Tests
{

    [TestClass]
    class TestSomeMethods
    {
        [TestMethod]

        public void BD_ExecuteScriptWithNoErrors() {


            try
            {
                Program.ejecutarScriptBD();
            }
            catch(Exception ex)
            {

                Assert.IsFalse(true);

            }
            finally {

                Assert.IsTrue(true);
            
            
            }





        
        }

    }
}
