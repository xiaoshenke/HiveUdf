package wuxian.me.hiveudf.util;


import com.google.common.base.Charsets;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;


/**
 * Created by wuxian on 30/3/2017.
 * <p>
 * http://outofmemory.cn/java/guava/IO/Files-operation
 */
public class FileUtil {

    private FileUtil() {
    }

    private static String currentPath;

    public static String getCurrentPath() {
        if (currentPath == null) {
            currentPath = System.getProperty("user.dir");
        }
        return currentPath;
    }

    public static boolean checkFileExist(String path) {
        checkNotNull(path);
        File file = new File(path);
        return file.exists();
    }

    public static boolean writeToFile(String path, String content) {
        checkNotNull(path);
        checkNotNull(content);
        File file = new File(path);
        try {
            if (!file.exists()) {
                Files.createParentDirs(file);
            }
            Files.write(content.getBytes(), file);
        } catch (IOException e) {
            file.delete();
            return false;
        }
        return true;
    }


    public static String readFromFile(String path) {
        checkNotNull(path);
        File file = new File(path);
        try {
            List<String> lines = Files.readLines(file, Charsets.UTF_8);
            String content = "";
            for (String line : lines) {
                content += line;
            }
            return content;
        } catch (IOException e) {

            return null;
        }
    }


    public static File getCurrentRunningFile() {
        try {
            //取当前jar做检查
            File file = new File(FileUtil.class.getProtectionDomain().
                    getCodeSource().getLocation().toURI().getPath());

            return file;
        } catch (Exception e) {

            return null;
        }
    }
}
