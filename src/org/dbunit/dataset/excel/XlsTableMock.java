package org.dbunit.dataset.excel;

import mockit.Delegate;
import mockit.Mocked;
import mockit.NonStrictExpectations;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;

/**
 * {@link XlsTable}をモック化するクラスです。<br/>
 * {@link XlsTable}はデフォルトアクセススコープなので、パッケージを合わせて定義しています。<br/>
 * <br/>
 * {@link XlsTable#getDateValue(HSSFCell)}でUTC時間を取るため、<br/>
 * Excelに記載する時間＋9時間（Timezoneオフセット）されますが、この仕様を無効化します。
 */
public class XlsTableMock {

	public static void mockup(){
		new NonStrictExpectations() {
			@Mocked(methods = {"getDateValue"})
			XlsTable xlsTable;
			{
				xlsTable.getDateValue(withInstanceOf(HSSFCell.class));
				//invoke(xlsTable, "getDateValue", HSSFCell.class);
				result = new Delegate<XlsTable>() {
					@SuppressWarnings("unused")
				    protected Object getDateValue(HSSFCell cell)
				    {
						//System.out.println("getDateValue");
				        //logger.debug("getDateValue(cell={}) - start", cell);
				        double numericValue = cell.getNumericCellValue();
				        java.util.Date date = HSSFDateUtil.getJavaDate(numericValue);
				        // Add the timezone offset again because it was subtracted automatically by Apache-POI (we need UTC)
				        // HACK start comment out.
				        //long tzOffset = TimeZone.getDefault().getOffset(date.getTime());
				        //date = new Date(date.getTime() + tzOffset);
				        // HACK end
				        return new Long(date.getTime());
				    }
				};
			}
		};
	}
}
