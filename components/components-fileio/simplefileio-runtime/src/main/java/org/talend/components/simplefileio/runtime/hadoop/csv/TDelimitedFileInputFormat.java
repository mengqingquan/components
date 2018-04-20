package org.talend.components.simplefileio.runtime.hadoop.csv;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;

public abstract class TDelimitedFileInputFormat<K, V> extends
    org.apache.hadoop.mapred.FileInputFormat<K, V> implements Configurable {

  static public String TALEND_SKIP_LINE_LENGTH = "talend.mapred.skip.line.length";

  public abstract RecordReader<K, V> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter) throws IOException;

  public Configuration getConf() {
    // only use setConf as init method
    return null;
  }

  private String inputPath;
  private int skipLines = 0;

  protected void setInputPath(String inputPath) {
    this.inputPath = inputPath;
  }

  protected void setSkipLines(int skipLines) {
    this.skipLines = skipLines;
  }

  protected org.apache.hadoop.fs.FileStatus[] listStatus(JobConf conf)
      throws IOException {
    org.apache.hadoop.fs.FileStatus[] status = super.listStatus(conf);
    java.util.List<org.apache.hadoop.fs.FileStatus> result = new java.util.ArrayList<org.apache.hadoop.fs.FileStatus>();

    for (int i = 0; status != null && i < status.length; i++) {
      org.apache.hadoop.fs.FileStatus statu = status[i];

      if (statu.isDir()) {
        continue;
      }
      result.add(statu);
    }
    return result.toArray(new org.apache.hadoop.fs.FileStatus[result.size()]);
  }

  /**
   * A factory that makes the split for this class. It can be overridden
   * by sub-classes to make sub-types
   */
  protected TFileSplit makeSplit(Path file, long start, long length, long skipLineLength,
      String[] hosts) {
    return new TFileSplit(file, start, length, skipLineLength, hosts);
  }

  public InputSplit[] getSplits(JobConf job, int numSplits)
      throws IOException {
    job.set("mapred.input.dir", inputPath);
    FileStatus[] files = listStatus(job);

    // Save the number of input files in the job-conf
    job.setLong("mapreduce.input.num.files", files.length);
    long totalSize = 0; // compute total size
    for (FileStatus file : files) { // check we have valid files
      if (file.isDir()) {
        throw new IOException("Not a file: " + file.getPath());
      }
      totalSize += file.getLen();
    }

    long goalSize = totalSize / (numSplits == 0 ? 1 : numSplits);
    long minSize = Math.max(job.getLong("mapred.min.split.size", 1), 1);

    // generate splits
    ArrayList<TFileSplit> splits = new ArrayList<TFileSplit>(numSplits);
    NetworkTopology clusterMap = new NetworkTopology();
    for (FileStatus file : files) {
      Path path = file.getPath();
      long skipLength = skipLines > 0 ? caculateSkipLength(file, job) : 0;

      job.setLong(TALEND_SKIP_LINE_LENGTH, skipLength);
      long length = file.getLen();
      if (length != 0) {
        FileSystem fs = path.getFileSystem(job);
        BlockLocation[] blkLocations = fs
            .getFileBlockLocations(file, 0, length);
        if (isSplitable(fs, path)) {
          long blockSize = file.getBlockSize();
          long splitSize = computeSplitSize(goalSize, minSize, blockSize);

          long bytesRemaining = length - skipLength;
          while (((double) bytesRemaining) / splitSize > 1.1) {
                  String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations,
                          length-bytesRemaining, splitSize, clusterMap);
                      splits.add(makeSplit(path, length-bytesRemaining, splitSize, skipLength,
                          splitHosts[0]));
            bytesRemaining -= splitSize;
          }

          if (bytesRemaining != 0) {
                  String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations, length
                          - bytesRemaining, bytesRemaining, clusterMap);
                      splits.add(makeSplit(path, length - bytesRemaining, bytesRemaining, skipLength,
                          splitHosts[0]));
          }
        } else {
          String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations, 0, length,clusterMap);
          splits.add(makeSplit(path, skipLength, length - skipLength, skipLength, splitHosts[0]));
        }
      } else {
        // Create empty hosts array for zero length files
        splits.add(new TFileSplit(path, 0, length, 0, new String[0]));
      }
    }
    LOG.debug("Total # of splits: " + splits.size());
    return splits.toArray(new TFileSplit[splits.size()]);
  }

  protected long caculateSkipLength(org.apache.hadoop.fs.FileStatus file,
      JobConf job) throws IOException {
    TFileSplit split = new TFileSplit(file.getPath(), 0, file.getLen(), 0,
        new String[0]);
    TDelimitedFileRecordReader reader = (TDelimitedFileRecordReader) getRecordReader(
        split, job, null);
    Text text = new Text();
    for (int i = 0; i < skipLines; i++) {
      reader.next(text);
    }
    return reader.getPos();
  }

  protected boolean isSplitable(FileSystem fs, Path filename) {
    CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(
        fs.getConf());
    CompressionCodec codec = compressionCodecs.getCodec(filename);
    return codec == null || codec instanceof SplittableCompressionCodec;
  }

  private String[][] getSplitHostsAndCachedHosts(BlockLocation[] blkLocations, long offset, long splitSize,
      NetworkTopology clusterMap) throws IOException {

    int startIndex = getBlockIndex(blkLocations, offset);

    long bytesInThisBlock = blkLocations[startIndex].getOffset() + blkLocations[startIndex].getLength() - offset;

    // If this is the only block, just return
    if (bytesInThisBlock >= splitSize) {
      return new String[][] { blkLocations[startIndex].getHosts(), blkLocations[startIndex].getHosts() };
    }

    long bytesInFirstBlock = bytesInThisBlock;
    int index = startIndex + 1;
    splitSize -= bytesInThisBlock;

    while (splitSize > 0) {
      bytesInThisBlock = Math.min(splitSize, blkLocations[index++].getLength());
      splitSize -= bytesInThisBlock;
    }

    long bytesInLastBlock = bytesInThisBlock;
    int endIndex = index - 1;

    Map<Node, NodeInfo> hostsMap = new IdentityHashMap<Node, NodeInfo>();
    Map<Node, NodeInfo> racksMap = new IdentityHashMap<Node, NodeInfo>();
    String[] allTopos = new String[0];

    // Build the hierarchy and aggregate the contribution of
    // bytes at each level. See TestGetSplitHosts.java

    for (index = startIndex; index <= endIndex; index++) {

      // Establish the bytes in this block
      if (index == startIndex) {
        bytesInThisBlock = bytesInFirstBlock;
      } else if (index == endIndex) {
        bytesInThisBlock = bytesInLastBlock;
      } else {
        bytesInThisBlock = blkLocations[index].getLength();
      }

      allTopos = blkLocations[index].getTopologyPaths();

      // If no topology information is available, just
      // prefix a fakeRack
      if (allTopos.length == 0) {
        allTopos = fakeRacks(blkLocations, index);
      }

      // NOTE: This code currently works only for one level of
      // hierarchy (rack/host). However, it is relatively easy
      // to extend this to support aggregation at different
      // levels

      for (String topo : allTopos) {

        Node node, parentNode;
        NodeInfo nodeInfo, parentNodeInfo;

        node = clusterMap.getNode(topo);

        if (node == null) {
          node = new NodeBase(topo);
          clusterMap.add(node);
        }

        nodeInfo = hostsMap.get(node);

        if (nodeInfo == null) {
          nodeInfo = new NodeInfo(node);
          hostsMap.put(node, nodeInfo);
          parentNode = node.getParent();
          parentNodeInfo = racksMap.get(parentNode);
          if (parentNodeInfo == null) {
            parentNodeInfo = new NodeInfo(parentNode);
            racksMap.put(parentNode, parentNodeInfo);
          }
          parentNodeInfo.addLeaf(nodeInfo);
        } else {
          nodeInfo = hostsMap.get(node);
          parentNode = node.getParent();
          parentNodeInfo = racksMap.get(parentNode);
        }

        nodeInfo.addValue(index, bytesInThisBlock);
        parentNodeInfo.addValue(index, bytesInThisBlock);

      } // for all topos

    } // for all indices

    // We don't yet support cached hosts when bytesInThisBlock > splitSize
    return new String[][] { identifyHosts(allTopos.length, racksMap), new String[0] };
  }

  private String[] identifyHosts(int replicationFactor, Map<Node, NodeInfo> racksMap) {

    String[] retVal = new String[replicationFactor];

    List<NodeInfo> rackList = new LinkedList<NodeInfo>();

    rackList.addAll(racksMap.values());

    // Sort the racks based on their contribution to this split
    sortInDescendingOrder(rackList);

    boolean done = false;
    int index = 0;

    // Get the host list for all our aggregated items, sort
    // them and return the top entries
    for (NodeInfo ni : rackList) {

      Set<NodeInfo> hostSet = ni.getLeaves();

      List<NodeInfo> hostList = new LinkedList<NodeInfo>();
      hostList.addAll(hostSet);

      // Sort the hosts in this rack based on their contribution
      sortInDescendingOrder(hostList);

      for (NodeInfo host : hostList) {
        // Strip out the port number from the host name
        retVal[index++] = host.node.getName().split(":")[0];
        if (index == replicationFactor) {
          done = true;
          break;
        }
      }

      if (done == true) {
        break;
      }
    }
    return retVal;
  }

  private void sortInDescendingOrder(List<NodeInfo> mylist) {
    Collections.sort(mylist, new Comparator<NodeInfo>() {
      public int compare(NodeInfo obj1, NodeInfo obj2) {

        if (obj1 == null || obj2 == null)
          return -1;

        if (obj1.getValue() == obj2.getValue()) {
          return 0;
        } else {
          return ((obj1.getValue() < obj2.getValue()) ? 1 : -1);
        }
      }
    });
  }

  private String[] fakeRacks(BlockLocation[] blkLocations, int index) throws IOException {
    String[] allHosts = blkLocations[index].getHosts();
    String[] allTopos = new String[allHosts.length];
    for (int i = 0; i < allHosts.length; i++) {
      allTopos[i] = NetworkTopology.DEFAULT_RACK + "/" + allHosts[i];
    }
    return allTopos;
  }

  private static class NodeInfo {
    final Node node;
    final Set<Integer> blockIds;
    final Set<NodeInfo> leaves;

    private long value;

    NodeInfo(Node node) {
      this.node = node;
      blockIds = new HashSet<Integer>();
      leaves = new HashSet<NodeInfo>();
    }

    long getValue() {
      return value;
    }

    void addValue(int blockIndex, long value) {
      if (blockIds.add(blockIndex) == true) {
        this.value += value;
      }
    }

    Set<NodeInfo> getLeaves() {
      return leaves;
    }

    void addLeaf(NodeInfo nodeInfo) {
      leaves.add(nodeInfo);
    }
  }
}
