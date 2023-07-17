using Apache.Arrow;
using System.Collections.Generic;
using System;
using NPOI.SS.UserModel;
using NPOI.XSSF.UserModel;
using System.IO;
using NPOI.SS.Util;
using NPOI.HSSF.UserModel;
using NPOI.SS.Formula.Functions;
using Org.BouncyCastle.X509;

namespace JsonFormatterTest
{
    public class ArrowExcelHelper
    {
        private Dictionary<string, ICellStyle> _cellStyleCache;
        private IDataFormat _dataFormat;

        public string SpreedSheetName { get; set; }

        public ArrowExcelHelper()
        {

        }

        public void Write(RecordBatch batch, Stream output, bool leaveOpen = false)
        {
            _cellStyleCache = new Dictionary<string, ICellStyle>();

            using IWorkbook workbook = new XSSFWorkbook();
            XSSFSheet sheet = (XSSFSheet)workbook.CreateSheet();
            _dataFormat = workbook.CreateDataFormat();

            CreateTable(batch, sheet);

            // Take into account the row containing column names
            for (int i = 0; i <= batch.Length; i++)
            {
                IRow row = sheet.CreateRow(i);
            }

            Schema schema = batch.Schema;
            IReadOnlyList<Field> fields = schema.FieldsList;

            for (int i = 0; i < fields.Count; i++)
            {
                IArrowArray column = batch.Column(i);
                IRow headingRow = sheet.GetRow(0);
                ICell headingCell = headingRow.CreateCell(i);
                SetValueAndFormat(headingCell, fields[i].Name, DataFormats.TEXT);

                if (column is DictionaryArray dictArray)
                {
                    IArrowArray realArray = dictArray.Dictionary;
                    ReadOnlySpan<int> indices = ((Int32Array)dictArray.Indices).Values;
                    Action<int, ICell> setter = GetCellValueSetter(realArray);

                    for (int j = 0; j < column.Length; j++)
                    {
                        IRow valueRow = sheet.GetRow(j + 1);
                        ICell cell = valueRow.CreateCell(i);
                        int index = indices[j];
                        setter(index, cell);
                    }

                    int width = (int)(GetColumnWidth(realArray) * 1.25f) * 256;
                    sheet.SetColumnWidth(i, Math.Max(width, 5120));
                }
                else
                {
                    Action<int, ICell> setter = GetCellValueSetter(column);

                    for (int j = 0; j < column.Length; j++)
                    {
                        IRow valueRow = sheet.GetRow(j + 1);
                        ICell cell = valueRow.CreateCell(i);
                        setter(j, cell);
                    }

                    int width = (int)(GetColumnWidth(column) * 1.25f) * 256;
                    sheet.SetColumnWidth(i, Math.Max(width, 5120));
                }
            }

            workbook.Write(output, leaveOpen);
            _cellStyleCache = null;
        }

        private XSSFTable CreateTable(RecordBatch batch, XSSFSheet sheet)
        {
            XSSFTable xssfTable = sheet.CreateTable();

            xssfTable.GetCTTable().id = 1;
            xssfTable.Name = "ArrowTable";
            xssfTable.IsHasTotalsRow = false;
            xssfTable.DisplayName = $"Table{xssfTable.GetCTTable().id}";

            var tableRange = new AreaReference(new CellReference(0, 0), new CellReference(batch.Length, batch.ColumnCount - 1));
            xssfTable.SetCellReferences(tableRange);

            xssfTable.StyleName = XSSFBuiltinTableStyleEnum.TableStyleMedium16.ToString();
            xssfTable.Style.IsShowColumnStripes = false;
            xssfTable.Style.IsShowRowStripes = true;

            IReadOnlyList<Field> fields = batch.Schema.FieldsList;

            for (int i = 0; i < fields.Count; i++)
            {
                xssfTable.CreateColumn(fields[i].Name, i);
            }

            // Add column filters
            xssfTable.GetCTTable().autoFilter = new()
            {
                @ref = tableRange.FormatAsString()
            };

            return xssfTable;
        }

        private Action<int, ICell> GetCellValueSetter(IArrowArray array)
        {
            return array switch
            {
                StringArray arr => (ix, cell) => FromStringArray(arr, ix, cell),
                Int8Array arr => (ix, cell) => FromInt8Array(arr, ix, cell),
                Int16Array arr => (ix, cell) => FromInt16Array(arr, ix, cell),
                Int32Array arr => (ix, cell) => FromInt32Array(arr, ix, cell),
                Int64Array arr => (ix, cell) => FromInt64Array(arr, ix, cell),
                UInt8Array arr => (ix, cell) => FromUInt8Array(arr, ix, cell),
                UInt16Array arr => (ix, cell) => FromUInt16Array(arr, ix, cell),
                UInt32Array arr => (ix, cell) => FromUInt32Array(arr, ix, cell),
                UInt64Array arr => (ix, cell) => FromUInt64Array(arr, ix, cell),
                Decimal128Array arr => (ix, cell) => FromDecimal128Array(arr, ix, cell),
                Decimal256Array arr => (ix, cell) => FromDecimal256Array(arr, ix, cell),
                BooleanArray arr => (ix, cell) => FromBooleanArray(arr, ix, cell),
                FloatArray arr => (ix, cell) => FromFloatArray(arr, ix, cell),
                DoubleArray arr => (ix, cell) => FromDoubleArray(arr, ix, cell),
                Time32Array arr => (ix, cell) => FromTime32Array(arr, ix, cell),
                Time64Array arr => (ix, cell) => FromTime64Array(arr, ix, cell),
                Date32Array arr => (ix, cell) => FromDate32Array(arr, ix, cell),
                Date64Array arr => (ix, cell) => FromDate64Array(arr, ix, cell),
                TimestampArray arr => (ix, cell) => FromTimeStampArray(arr, ix, cell),
                _ => (ix, cell) => { }
            };
        }

        private void FromDoubleArray(DoubleArray doubleArray, int index, ICell cell)
        {
            double? optional = doubleArray.GetValue(index);
            SetValueAndFormat(cell, optional, DataFormats.FLOATING);
        }

        private void FromFloatArray(FloatArray floatArray, int index, ICell cell)
        {
            float? optional = floatArray.GetValue(index);
            SetValueAndFormat(cell, optional, DataFormats.FLOATING);
        }

        private void FromInt8Array(Int8Array int8Array, int index, ICell cell)
        {
            sbyte? optional = int8Array.GetValue(index);
            SetValueAndFormat(cell, optional, DataFormats.INTEGER);

        }

        private void FromInt16Array(Int16Array int16Array, int index, ICell cell)
        {
            short? optional = int16Array.GetValue(index);
            SetValueAndFormat(cell, optional, DataFormats.INTEGER);
        }

        private void FromInt32Array(Int32Array int32Array, int index, ICell cell)
        {
            int? optional = int32Array.GetValue(index);
            SetValueAndFormat(cell, optional, DataFormats.INTEGER);
        }

        private void FromInt64Array(Int64Array int64Array, int index, ICell cell)
        {
            long? optional = int64Array.GetValue(index);
            SetValueAndFormat(cell, optional, DataFormats.INTEGER);
        }

        private void FromUInt8Array(UInt8Array uint8Array, int index, ICell cell)
        {
            byte? optional = uint8Array.GetValue(index);
            SetValueAndFormat(cell, optional, DataFormats.INTEGER);
        }

        private void FromUInt16Array(UInt16Array uint16Array, int index, ICell cell)
        {
            ushort? optional = uint16Array.GetValue(index);
            SetValueAndFormat(cell, optional, DataFormats.INTEGER);

        }

        private void FromUInt32Array(UInt32Array uint32Array, int index, ICell cell)
        {
            uint? optional = uint32Array.GetValue(index);
            SetValueAndFormat(cell, optional, DataFormats.INTEGER);
        }

        private void FromUInt64Array(UInt64Array uint64Array, int index, ICell cell)
        {
            ulong? optional = uint64Array.GetValue(index);
            SetValueAndFormat(cell, optional, DataFormats.INTEGER);
        }

        private void FromDecimal128Array(Decimal128Array decimal128Array, int index, ICell cell)
        {
            decimal? optional = decimal128Array.GetValue(index);

            if (optional.HasValue)
            {
                SetValueAndFormat(cell, decimal.ToDouble(optional.Value), DataFormats.FLOATING);
            }
            else
            {
                cell.SetBlank();
            }
        }

        private void FromDecimal256Array(Decimal256Array decimal256Array, int index, ICell cell)
        {
            decimal? optional = decimal256Array.GetValue(index);

            if (optional.HasValue)
            {
                SetValueAndFormat(cell, decimal.ToDouble(optional.Value), DataFormats.FLOATING);
            }
            else
            {
                cell.SetBlank();
            }
        }

        private void FromTime32Array(Time32Array time32Array, int index, ICell cell)
        {
            int? optional = time32Array.GetValue(index); // milliseconds

            if (optional.HasValue)
            {
                TimeSpan ts = TimeSpan.FromMilliseconds(optional.Value);
                cell.SetCellValue(ts.ToString());
                SetCellStyle(cell, DataFormats.TIME);
            }
            else
            {
                cell.SetBlank();
            }
        }

        private void FromTime64Array(Time64Array time64Array, int index, ICell cell)
        {
            long? optional = time64Array.GetValue(index);
            SetValueAndFormat(cell, optional, DataFormats.TIME);
        }

        private void FromDate32Array(Date32Array date32Array, int index, ICell cell)
        {
            DateTime? optional = date32Array.GetDateTime(index);
            SetValueAndFormat(cell, optional, DataFormats.DATE);
        }

        private void FromDate64Array(Date64Array date64Array, int index, ICell cell)
        {
            DateTime? optional = date64Array.GetDateTime(index);
            SetValueAndFormat(cell, optional, DataFormats.DATE);
        }

        private void FromTimeStampArray(TimestampArray timestampArray, int index, ICell cell)
        {
            DateTimeOffset? optional = timestampArray.GetTimestamp(index);
            SetValueAndFormat(cell, optional, DataFormats.DATETIME);
        }

        private void FromStringArray(StringArray stringArray, int index, ICell cell)
        {
            string value = stringArray.GetString(index);
            SetValueAndFormat(cell, value, DataFormats.TEXT);
        }

        private void FromBooleanArray(BooleanArray boolArray, int index, ICell cell)
        {
            bool? optional = boolArray.GetValue(index);
            SetValueAndFormat(cell, optional, DataFormats.GENERAL);
        }

        private void SetValueAndFormat(ICell cell, bool? optional, string userFormat)
        {
            if (optional.HasValue)
            {
                cell.SetCellValue(optional.Value);
                SetCellStyle(cell, userFormat);
            }
            else
            {
                cell.SetBlank();
            }
        }

        private void SetValueAndFormat(ICell cell, string optional, string userFormat)
        {
            if (optional != null)
            {
                cell.SetCellValue(optional);
                SetCellStyle(cell, userFormat);
            }
            else
            {
                cell.SetBlank();
            }
        }

        private void SetValueAndFormat(ICell cell, double? value, string userFormat)
        {
            if (value.HasValue && value.Value != double.NaN)
            {
                cell.SetCellValue(value.Value);
                SetCellStyle(cell, userFormat);
            }
            else
            {
                cell.SetBlank();
            }
        }

        private void SetValueAndFormat(ICell cell, DateTime? optional, string userFormat)
        {
            if (optional.HasValue)
            {
                cell.SetCellValue(optional.Value);
                SetCellStyle(cell, userFormat);
            }
            else
            {
                cell.SetBlank();
            }
        }

        private void SetValueAndFormat(ICell cell, DateTimeOffset? optional, string userFormat)
        {
            if (optional.HasValue)
            {
                cell.SetCellValue(optional.Value.DateTime);
                SetCellStyle(cell, userFormat);
            }
            else
            {
                cell.SetBlank();
            }
        }

        private void SetCellStyle(ICell cell, string userFormat)
        {
            if (!_cellStyleCache.TryGetValue(userFormat, out ICellStyle style))
            {
                style = cell.Sheet.Workbook.CreateCellStyle();

                short index = HSSFDataFormat.GetBuiltinFormat(userFormat);

                if (index != -1)
                {
                    style.DataFormat = index;
                }
                else
                {
                    style.DataFormat = _dataFormat.GetFormat(userFormat);
                }

                _cellStyleCache.Add(userFormat, style);
            }
            
            style.Alignment = HorizontalAlignment.Center;
            cell.CellStyle = style;
        }

        private int GetColumnWidth(IArrowArray array)
        { 
            return array switch
            {
                StringArray arr => GetLongestWidth(arr),
                Int8Array => 3,
                Int16Array => 5,
                Int32Array => 13,
                Int64Array => 19,
                UInt8Array => 3,
                UInt16Array => 5,
                UInt32Array => 13,
                UInt64Array => 19,
                FloatArray => 14,
                DoubleArray => 23,
                Decimal128Array => 23,
                Decimal256Array => 23,
                Time32Array => 5,
                Time64Array => 5,
                Date32Array => 20,
                Date64Array => 20,
                BooleanArray => 4,
                _ => 0,
            };
        }

        private int GetLongestWidth(StringArray stringArray)
        {
            if (stringArray.Length < 1)
                return int.MinValue;

            int maxLength = stringArray.ValueOffsets[1] - stringArray.ValueOffsets[0];
            

            for(int i = stringArray.Length; i > 1; i--)
            {
                int difference = stringArray.ValueOffsets[i] - stringArray.ValueOffsets[i - 1];

                if(difference > maxLength)
                {
                    maxLength = difference;
                }
            }

            return maxLength;
        }

        private int GetLongestWidth(Decimal128Array decimalArray)
        {
            if(decimalArray.Length < 1)
                return int.MinValue;

            decimal max = decimal.MinValue;

            for(int i=0; i<decimalArray.Length; i++)
            {
                var value = decimalArray.GetValue(i);

                if(value.HasValue && value.Value > max)
                {
                    max = value.Value;
                }
            }

            return new DecimalHelper().GetDigits(ref max);
        }

        private int GetLongestWidth(Decimal256Array decimalArray)
        {
            if (decimalArray.Length < 1)
                return int.MinValue;

            decimal max = decimal.MinValue;

            for (int i = 0; i < decimalArray.Length; i++)
            {
                var value = decimalArray.GetValue(i);

                if (value.HasValue && value.Value > max)
                {
                    max = value.Value;
                }
            }

            return new DecimalHelper().GetDigits(ref max);
        }

        private int GetLongestWidth<T>(PrimitiveArray<T> array) where T: struct, IComparable<T>
        {
            if (array.Length < 1)
                return int.MinValue;

            T max = default;
            ReadOnlySpan<T> values = array.Values;

            for (int i=1; i< array.Length; i++)
            {
                if (values[i].CompareTo(max) > 0)
                {
                    max = values[i];
                }
            }

            decimal absMaxNumber = Convert.ToDecimal(max);

            return new DecimalHelper().GetDigits(ref absMaxNumber);
        }

    }
}
