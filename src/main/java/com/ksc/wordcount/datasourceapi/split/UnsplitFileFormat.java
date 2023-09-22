package com.ksc.wordcount.datasourceapi.split;

import com.ksc.wordcount.datasourceapi.reader.PartionReader;
import com.ksc.wordcount.datasourceapi.reader.TextPartionReader;
import com.ksc.wordcount.datasourceapi.writer.PartionWriter;
import com.ksc.wordcount.datasourceapi.writer.TextPartionWriter;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class UnsplitFileFormat implements FileFormat {

        @Override
        public boolean isSplitable(String filePath) {
            return false;
        }

    /**
     * 输入: 单个文件 | 文件夹
     */
        @Override
        public PartionFile[] getSplits(String filePath, long size) {
            File target = new File(filePath);
            if (target.isFile()) {
                // 文件则只切成一个
                return new PartionFile[] { new PartionFile(0, new FileSplit[] {
                        new FileSplit(filePath, 0, target.length())
                })};
            }
            List<PartionFile> partionFileList = new ArrayList<>();

            //todo 学生实现 driver端切分split的逻辑 - finish

            File[] files = target.listFiles();
            if (files == null || files.length == 0) {
                return partionFileList.toArray(new PartionFile[0]);
            }
            // 将文件夹下的每个文件生成对应的 PartionFile
            int partId = 0; // 用于标记 partionId
            for (File file : files) {
                FileSplit[] fileSplit = {new FileSplit(file.getAbsolutePath(), 0, file.length())};
                partionFileList.add(new PartionFile(partId++, fileSplit));
            }
            return partionFileList.toArray(new PartionFile[0]);
        }

    @Override
    public PartionReader createReader() {
        return new TextPartionReader();
    }

    @Override
    public PartionWriter createWriter(String destPath, int partionId) {
        return new TextPartionWriter(destPath, partionId);
    }


}
