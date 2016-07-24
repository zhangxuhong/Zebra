/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hama.bsp;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Metis Partition.
 * 
 */
public class MetisPartitioner<K, V> implements Partitioner<K, V> {
  

  private Configuration conf;
  
  private int numTasksNow; // which numTask's cache
  private static String FILE_PARENT="/metis"; // e.g. "hdfs://brick0:54310/metis";
  private static String FILE_PREFIX="graph.part.";
  private Map<Long, Integer> cache;
  
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }
  
  @Override
  public Configuration getConf() {
    return this.conf;
  }
  
  private Map<Long, Integer> getCache(int numTasks) throws IOException{
    if (cache == null){
      cache = new HashMap<Long, Integer>();
    }
    if (numTasksNow != numTasks){
      cache.clear();
      readPatitionResultFromHDFS(numTasks, cache);
      numTasksNow = numTasks;
    }
    return cache;
    
  }

  private void readPatitionResultFromHDFS(int numTasks, Map<Long, Integer> cache) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    // Specifies a new file in HDFS.
    String metisResut = FILE_PARENT + "/" + FILE_PREFIX + numTasks;
    Path filenamePath = new Path(metisResut);
    // if the file already exists delete it.
    if (!fs.exists(filenamePath)) {
      throw new IOException("Metis Partition Result doesnot exist for tasknumber:\n" + numTasks+ " file path:"+filenamePath);
    }

    // FSInputStream to read out of the filenamePath file
    FSDataInputStream fout = fs.open(filenamePath);
    long idxLine = 1;
    while (true){
      @SuppressWarnings("deprecation")
      String line = fout.readLine();
      if (line == null){
        break;
      }
      cache.put(idxLine++, Integer.parseInt(line));
    }

    fout.close();

  }
  public static void main(String argv[]){
    MetisPartitioner<String, String> p = new MetisPartitioner<String, String>();
    System.out.println(p.getPartition("1", null, 12)+"hahfasdfasd")     ;
  }

  @Override
  public int getPartition(K key, V value, int numTasks) {
    long k = Long.parseLong(key.toString());
    int partition = -1;
    try {
      partition = getCache(numTasks).get(k);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      System.exit(-1);
    }
    return partition;
  }

}
