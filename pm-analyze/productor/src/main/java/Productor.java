import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFReader;

import java.io.FileInputStream;
import java.io.InputStream;

/**
 * Created by Hanyue on 2016/1/21.
 */
public class Productor {
    public static void main(String[] args) {
        System.console().printf("hello world");

    }

    public static void ReadDBF(String path) {
        InputStream fis = null;
        try {

            fis = new FileInputStream(path);

            DBFReader reader = new DBFReader(fis);

            int fieldsCount = reader.getFieldCount();

            for (int i = 0; i < fieldsCount; i++) {
                DBFField field = reader.getField(i);
                System.out.println(field.getName());
            }
            Object[] rowValues;

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
