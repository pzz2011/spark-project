import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFReader;

import java.io.FileInputStream;
import java.io.InputStream;

/**
 * Created by Hanyue on 2016/1/21.
 */
public class Productor {
    public static void main(String[] args) {
        ReadDBF("C:\\Users\\Administrator\\Downloads\\BJ_CHECKINS_SINA_WEIBO.shp\\BJ_CHECKINS_SINA_WEIBO.dbf");

    }

    public static void ReadDBF(String path) {
        InputStream fis = null;
        try {
            // ��ȡ�ļ���������
            fis = new FileInputStream(path);
            // ������������ʼ��һ��DBFReaderʵ����������ȡDBF�ļ���Ϣ
            DBFReader reader = new DBFReader(fis);
            // ����DBFReader��ʵ�������õ�path�ļ����ֶεĸ���
            int fieldsCount = reader.getFieldCount();
            // ȡ���ֶ���Ϣ
            for (int i = 0; i < fieldsCount; i++) {
                DBFField field = reader.getField(i);
                System.out.println(field.getName());
            }
            Object[] rowValues;
            // һ����ȡ��path�ļ��м�¼
            while ((rowValues = reader.nextRecord()) != null) {
                for (int i = 0; i < rowValues.length; i++) {
                    System.out.println(rowValues[i]);
                    Thread.sleep(8000);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                fis.close();
            } catch (Exception e) {
            }
        }
    }
}
