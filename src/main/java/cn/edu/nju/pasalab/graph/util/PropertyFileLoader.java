package cn.edu.nju.pasalab.graph.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by wangzhaokang on 3/15/17.
 */
public class PropertyFileLoader {

    public static Properties load(Properties p, String filename) {
        if (new File(filename).exists()) {
            try {
                FileInputStream fin = new FileInputStream(filename);
                p.load(fin);
                fin.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return p;
    }

    public static Properties load(String filename) {
        Properties p = new Properties();
        if (new File(filename).exists()) {
            try {
                FileInputStream fin = new FileInputStream(filename);
                p.load(fin);
                fin.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return p;
    }
}
