package org.dbunit.dataset.excel;

import mockit.Delegate;
import mockit.Mocked;
import mockit.NonStrictExpectations;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;

/**
 * {@link XlsTable}�����b�N������N���X�ł��B<br/>
 * {@link XlsTable}�̓f�t�H���g�A�N�Z�X�X�R�[�v�Ȃ̂ŁA�p�b�P�[�W�����킹�Ē�`���Ă��܂��B<br/>
 * <br/>
 * {@link XlsTable#getDateValue(HSSFCell)}��UTC���Ԃ���邽�߁A<br/>
 * Excel�ɋL�ڂ��鎞�ԁ{9���ԁiTimezone�I�t�Z�b�g�j����܂����A���̎d�l�𖳌������܂��B
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
