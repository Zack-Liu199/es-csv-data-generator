# es-csv-data--generator


nohup java -Xmx16g -Xms16g -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:G1HeapRegionSize=16M -XX:+ParallelRefProcEnabled -XX:+PerfDisableSharedMem -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/heapdump.hprof -jar ES-data-generator-1.0-SNAPSHOT.jar config.properties > nohup.log &

参数说明：

-Xmx16g -Xms16g：将堆内存固定为 16GB，避免动态调整带来的性能波动
-XX:+UseG1GC：使用 G1 垃圾回收器，适合大内存和高吞吐量场景
-XX:MaxGCPauseMillis=100：设置最大 GC 停顿时间为 100 毫秒
-XX:G1HeapRegionSize=16M：增大 G1 区域大小，减少碎片
-XX:+ParallelRefProcEnabled：并行处理引用对象，加速 GC
-XX:+PerfDisableSharedMem：禁用性能数据共享内存，减少内存占用
-XX:+HeapDumpOnOutOfMemoryError：OOM 时生成堆转储文件，方便事后分析

### 其他优化建议
1. **使用 SSD 存储**：机械硬盘在处理连续大量写入时性能会急剧下降，SSD 能提供更好的 IO 性能
2. **并行生成**：通过多线程或多进程并行生成不同分片，充分利用多核 CPU
3. **优化 IO 操作**：
    - 使用`BufferedWriter`并增大缓冲区
    - 考虑使用`FileChannel`进行零拷贝 IO
4. **监控与调优**：
    - 使用`jstat`、`jvisualvm`或`JProfiler`监控 GC 和内存使用情况
    - 根据监控结果调整堆大小和 GC 参数