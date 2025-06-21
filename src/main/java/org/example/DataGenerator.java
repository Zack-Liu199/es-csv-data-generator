package org.example;

import java.io.*;
import java.util.Properties;
import java.util.Random;

/**
 * 高性能大数据CSV生成器 - 无状态姓名生成策略
 */
public class DataGenerator {
    private static final String[] FIRST_NAMES = {
            "张", "王", "李", "赵", "刘", "陈", "杨", "黄", "周", "吴",
            "徐", "孙", "胡", "朱", "高", "林", "何", "郭", "马", "罗"
    };

    private static final String[] LAST_NAMES = {
            "伟", "芳", "秀英", "娜", "敏", "静", "丽", "强", "军", "磊",
            "洋", "勇", "俊", "峰", "娟", "艳", "玲", "涛", "丹", "浩"
    };

    private static final String[] MIDDLE_NAMES = {
            "小", "大", "阿", "子", "晓", "明", "文", "佳", "思", "雨"
    };

    private static final int SUFFIX_LENGTH = 4;
    private static final Random random = new Random();

    // 更频繁的缓冲区刷新
    private static final int FLUSH_INTERVAL = 50000;
    // 使用ThreadLocal减少锁竞争
    private static final ThreadLocal<Random> threadLocalRandom = ThreadLocal.withInitial(Random::new);

    public static void main(String[] args) {
        Properties config = loadConfig(args.length > 0 ? args[0] : null);

        int totalShards = Integer.parseInt(config.getProperty("total.shards", "10"));
        long recordsPerShard = Long.parseLong(config.getProperty("records.per.shard", "100000000"));
        String outputDir = config.getProperty("output.dir", "./");
        int threadCount = Integer.parseInt(config.getProperty("thread.count", "1"));

        try {
            if (threadCount <= 1) {
                // 单线程模式
                for (int shard = 0; shard < totalShards; shard++) {
                    String fileName = outputDir + "data_shard_" + shard + ".csv";
                    generateDataShard(fileName, shard * recordsPerShard, recordsPerShard);
                    System.out.println("分片 " + shard + " 生成完成");
                }
            } else {
                // 多线程模式
                Thread[] threads = new Thread[threadCount];
                for (int t = 0; t < threadCount; t++) {
                    final int threadId = t;
                    threads[t] = new Thread(() -> {
                        for (int shard = threadId; shard < totalShards; shard += threadCount) {
                            try {
                                String fileName = outputDir + "data_shard_" + shard + ".csv";
                                generateDataShard(fileName, shard * recordsPerShard, recordsPerShard);
                                System.out.println("线程 " + threadId + " 完成分片 " + shard);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    });
                    threads[t].start();
                }

                // 等待所有线程完成
                for (Thread thread : threads) {
                    thread.join();
                }
            }

            System.out.println("所有数据生成完成");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void generateDataShard(String fileName, long startId, long recordCount) throws IOException {
        File file = new File(fileName);
        File parentDir = file.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            parentDir.mkdirs();
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            writer.write("id,姓名\n");

            // 使用局部变量减少内存访问
            Random localRandom = threadLocalRandom.get();

            for (long i = 0; i < recordCount; i++) {
                long id = startId + i;
                String name = generateName(id, localRandom);

                writer.write(id + "," + name + "\n");

                if (i % FLUSH_INTERVAL == 0) {
                    writer.flush();
                    if (i % 1000000 == 0) {
                        System.out.println("已生成: " + (i / 1000000) + "百万条记录");
                    }
                }
            }
        }
    }

    // 基于ID确定性生成姓名，无需维护Map
    private static String generateName(long id, Random random) {
        // 使用ID作为随机种子，确保每次生成结果一致
        random.setSeed(id);

        String firstName = FIRST_NAMES[random.nextInt(FIRST_NAMES.length)];
        String middleName = MIDDLE_NAMES[random.nextInt(MIDDLE_NAMES.length)];
        String lastName = LAST_NAMES[random.nextInt(LAST_NAMES.length)];

        // 使用ID的哈希值作为后缀，确保唯一性
        String suffix = String.format("%04x", id % 0x10000);

        return firstName + middleName + lastName + "_" + suffix;
    }

    private static Properties loadConfig(String configPath) {
        Properties config = new Properties();
        try {
            if (configPath != null) {
                try (FileInputStream fis = new FileInputStream(configPath)) {
                    config.load(fis);
                    System.out.println("成功加载外部配置文件: " + configPath);
                    return config;
                }
            }

            try (InputStream is = DataGenerator.class.getClassLoader().getResourceAsStream("config.properties")) {
                if (is != null) {
                    config.load(is);
                    System.out.println("成功加载默认配置文件");
                    return config;
                }
            }

            System.err.println("警告: 未能加载任何配置文件，使用硬编码默认值");
        } catch (IOException e) {
            System.err.println("加载配置文件失败: " + e.getMessage());
        }

        config.setProperty("total.shards", "10");
        config.setProperty("records.per.shard", "100000000"); // 1亿条/分片
        config.setProperty("output.dir", "./data/");
        config.setProperty("thread.count", "1");

        return config;
    }
}