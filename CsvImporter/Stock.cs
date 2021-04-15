using System;
using System.Collections.Generic;
using System.Text;

namespace CsvImporter
{
    public class SaleStock
    {
        public string PointOfSale { get; set; }
        public string Product { get; set; }
        public DateTime Date { get; set; }
        public int Stock { get; set; }
    }
}
