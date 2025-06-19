package org.example;

import java.io.*;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ClassName: DataGenerator
 * Package: PACKAGE_NAME
 * Description:
 *
 * @Author Zack_Liu
 * @Create 2025/6/14 10:31
 * @Version 1.0
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

    private static final String[] MIDDLE_NAMES = {  // 新增中间名（可选）
            "小", "大", "阿", "子", "晓", "明", "文", "佳", "思", "雨"
    };
    private static final int SUFFIX_LENGTH = 4;  // 后缀随机字符长度


    private static final String[] KEYWORDS = {
            "技术", "研发", "产品", "市场", "销售", "运营", "设计", "测试",
            "管理", "财务", "人事", "行政", "客服", "数据", "算法", "架构"
    };

    private static final String[] DESCRIPTIONS = {
            "专注于企业级解决方案", "致力于技术创新", "打造优质用户体验",
            "推动数字化转型", "数据驱动决策", "高效团队协作",
            "前沿技术研究", "行业领先方案"
    };

    private static final int UNIQUE_SUFFIX_LENGTH = 4; // 随机后缀长度
    private static final int MAX_NAME_COUNT = 10000;   // 每个姓名最大出现次数
    private static final Map<String, Integer> nameCountMap = new ConcurrentHashMap<>();
    private static final Random random = new Random();


    public static void main(String[] args) {
        // 读取配置文件
        String configPath = null;
        if (args.length > 0) {
            configPath = args[0];
        }

        Properties config = loadConfig(configPath);

        // 从配置中获取参数
        int totalShards = Integer.parseInt(config.getProperty("total.shards", "10"));
        long recordsPerShard = Long.parseLong(config.getProperty("records.per.shard", "10000"));
        String outputDir = config.getProperty("output.dir", "./");

        try {
                for (int shard = 0; shard < totalShards; shard++) {
                    String fileName = "data_shard_" + shard + ".csv";
                    generateDataShard(fileName, shard * recordsPerShard, recordsPerShard);
                    System.out.println("分片 " + shard + " 生成完成");
                }

                System.out.println("所有数据生成完成");
            } catch (IOException e) {
                e.printStackTrace();
            }
    }
//
private static void generateDataShard(String fileName, long startId, long recordCount) throws IOException {
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
        // 写入CSV表头（只保留id和姓名）
        writer.write("id,姓名\n");

        for (long i = 0; i < recordCount; i++) {
            long id = startId + i;
            String name = generateControlledName();

            // 写入数据行（只包含id和姓名）
            writer.write(id + "," + name + "\n");

            // 定期刷新缓冲区，避免内存溢出
            if (i % 1000000 == 0) {
                writer.flush();
                System.out.println("已生成: " + (i / 1000000) + "百万条记录");
            }
        }
    }
}

    // 生成受控的随机姓名，确保每个姓名出现次数不超过MAX_NAME_COUNT
    private static String generateControlledName() {
        while (true) {
            String firstName = FIRST_NAMES[random.nextInt(FIRST_NAMES.length)];
            String middleName = MIDDLE_NAMES[random.nextInt(MIDDLE_NAMES.length)];
            String lastName = LAST_NAMES[random.nextInt(LAST_NAMES.length)];

            // 基础姓名格式：张小明
            String baseName = firstName + middleName + lastName;

            // 添加随机后缀，增加唯一性
            StringBuilder suffix = new StringBuilder();
            for (int i = 0; i < UNIQUE_SUFFIX_LENGTH; i++) {
                suffix.append(Character.forDigit(random.nextInt(36), 36));
            }
            String fullName = baseName + "_" + suffix.toString();

            // 使用CAS操作控制每个姓名的生成次数
            nameCountMap.compute(fullName, (k, v) -> v == null ? 1 : v + 1);
            int count = nameCountMap.get(fullName);

            if (count <= MAX_NAME_COUNT) {
                return fullName;
            } else {
                // 如果已达到最大次数，尝试生成另一个
                nameCountMap.put(fullName, MAX_NAME_COUNT); // 防止其他线程重复计数
            }
        }
    }

        private static String generateRandomName() {
            String firstName = FIRST_NAMES[random.nextInt(FIRST_NAMES.length)];
            String lastName = LAST_NAMES[random.nextInt(LAST_NAMES.length)];
            return firstName + lastName;
        }
        private static Properties loadConfig(String configPath) {
            Properties config = new Properties();
            try {
                if (configPath != null) {
                    // 尝试从指定路径加载配置
                    try (FileInputStream fis = new FileInputStream(configPath)) {
                        config.load(fis);
                        System.out.println("成功加载外部配置文件: " + configPath);
                        return config;
                    }
                }

                // 如果未指定或加载失败，尝试从classpath加载默认配置
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
            return config;
        }
    }