package es.udc.fi.ri.practicari;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.*;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.demo.knn.DemoEmbeddings;
import org.apache.lucene.demo.knn.KnnVectorDict;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.IOUtils;

class WorkerThread implements Runnable {

  private final Path folder;
  private final boolean create;
  private final boolean contentsStored;
  private final boolean contentsTermVectors;
  private final int depth;
  private final IndexFiles indexFiles;
  private final String[] onlyFiles;
  private final String[] notFiles;
  private final int nlines;
  //AÑADIR ATRIBUTO PARA LAS KEYS DE CONFIG.PROPERTIES

  public WorkerThread(final Path folder, final boolean create, final int depth, final boolean contentsStored, final boolean contentsTermVectors, final IndexFiles indexFiles,  String[] onlyFiles, String[] notFiles, int nlines) {
    this.folder = folder;
    this.create = create;
    this.depth = depth;
    this.contentsStored = contentsStored;
    this.contentsTermVectors = contentsTermVectors;
    this.indexFiles = indexFiles;
    this.onlyFiles = onlyFiles;
    this.notFiles = notFiles;
    this.nlines = nlines;
  }

  /**
   * This is the work that the current thread will do when processed by the pool.
   * In this case, it will only print some information.
   */
  @Override
  public void run() {
    System.out.printf("Soy el hilo '%s' y voy a indexar las entradas de la carpeta '%s'%n",
            Thread.currentThread().getName(), folder);
    Analyzer analyzer = new StandardAnalyzer();
    IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
    if (create) {
      // Create a new index in the directory, removing any
      // previously indexed documents:
      iwc.setOpenMode(OpenMode.CREATE);
    } else {
      // Add new documents to an existing index:
      iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
    }
    Directory dir = null;
    try {
      dir = FSDirectory.open(Paths.get(Thread.currentThread().getName() + "-" + folder.getFileName()));
    } catch (IOException e) {
      e.printStackTrace();
    }
    try (IndexWriter iwriter = new IndexWriter(dir, iwc)) {
      indexFiles.indexDocs(iwriter, folder, depth, contentsStored, contentsTermVectors, onlyFiles, notFiles, nlines);
      indexFiles.addDir(dir);
      System.out.printf("Soy el hilo '%s' y he acabado de indexar las entradas de la carpeta '%s'%n",
              Thread.currentThread().getName(), folder);
    } catch (IOException e) {
      System.out.println(" caught a " + e.getClass() + "\n with message: " + e.getMessage());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}

/**
 * Index all text files under a directory.
 *
 * <p>This is a command-line application demonstrating simple Lucene indexing. Run it with no
 * command-line arguments for usage information.
 */
public class IndexFiles implements AutoCloseable {
  static final String KNN_DICT = "knn-dict";

  // Calculates embedding vectors for KnnVector search
  private final DemoEmbeddings demoEmbeddings;
  private final KnnVectorDict vectorDict;
  private List<Directory> dirList;

  /* Indexed, tokenized, stored. */
  public static final FieldType FIELD_TYPE = new FieldType();

  static final IndexOptions options = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;

  static {
    FIELD_TYPE.setIndexOptions(options);
    FIELD_TYPE.setTokenized(true);
  }

  public void addDir(Directory dir) {
      dirList.add(dir);
  }


  private IndexFiles(KnnVectorDict vectorDict) throws IOException {
    if (vectorDict != null) {
      this.vectorDict = vectorDict;
      demoEmbeddings = new DemoEmbeddings(vectorDict);
    } else {
      this.vectorDict = null;
      demoEmbeddings = null;
    }
    dirList = new ArrayList<Directory>();
  }

  public List<Directory> getDirList() {
    return dirList;
  }

  public static String sha256(String input) throws NoSuchAlgorithmException {
    MessageDigest md = MessageDigest.getInstance("SHA-256");
    byte[] hash = md.digest(input.getBytes(StandardCharsets.UTF_8));
    StringBuilder hexString = new StringBuilder();
    for (byte b : hash) {
      String hex = Integer.toHexString(0xff & b);
      if (hex.length() == 1) hexString.append('0');
      hexString.append(hex);
    }
    return hexString.toString();
  }

  /** Index all text files under a directory. */
  public static void main(String[] args) throws Exception {
    String usage =
        "java org.apache.lucene.demo.IndexFiles"
            + " [-index INDEX_PATH] [-docs DOCS_PATH] [-update] [-knn_dict DICT_PATH] [-numThreads NUM] [-depth NUM] [-contentsStored] [-contentsTermVectors]\n\n"
            + "This indexes the documents in DOCS_PATH, creating a Lucene index"
            + "in INDEX_PATH that can be searched with SearchFiles\n"
            + "IF DICT_PATH contains a KnnVector dictionary, the index will also support KnnVector search";
    String indexPath = "index";
    String docsPath = null;
    String vectorDictSource = null;
    int numThreads = 0;
    int depth = -1;
    boolean create = true;
    boolean contentsStored = false;
    boolean contentsTermVectors = false;
    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "-index":
          indexPath = args[++i];
          break;
        case "-docs":
          docsPath = args[++i];
          break;
        case "-knn_dict":
          vectorDictSource = args[++i];
          break;
        case "-update":
          create = false;
          break;
        case "-create":
          create = true;
          break;
        case "-numThreads":
          numThreads = Integer.parseInt(args[++i]);
          break;
        case "-depth":
          depth = Integer.parseInt(args[++i]);
          break;
        case "-contentsStored":
          contentsStored = true;
          break;
        case "-contentsTermVectors":
          contentsTermVectors = true;
          break;
        default:
          throw new IllegalArgumentException("unknown parameter " + args[i]);
      }
    }

    if (numThreads <= 0) {
      numThreads = Runtime.getRuntime().availableProcessors();
    }

    if (depth == 0) {
      System.exit(0);
    }

    if (docsPath == null) {
      System.err.println("Usage: " + usage);
      System.exit(1);
    }

    //Convierte la ruta de String a Path
    final Path docDir = Paths.get(docsPath);
    if (!Files.isReadable(docDir)) {
      System.out.println(
          "Document directory '"
              + docDir.toAbsolutePath()
              + "' does not exist or is not readable, please check the path");
      System.exit(1);
    }

    Properties properties = new Properties();
    properties.load(new FileReader("./src/main/resources/config.properties"));

    String onlyFilesString = properties.getProperty("onlyFiles");
    String notFilesString = properties.getProperty("notFiles");
    String onlyLines = properties.getProperty("onlyLines");

    if (notFilesString != null && !notFilesString.isEmpty() && !notFilesString.matches("(\\.\\w+\\s?)+")) {
      throw new Exception("notFiles no tiene extensiones de archivo válidas.");
    }
    if (onlyFilesString != null && !onlyFilesString.isEmpty() && !onlyFilesString.matches("(\\.\\w+\\s?)+")) {
      throw new Exception("onlyFiles no tiene extensiones de archivo válidas.");
    }

    // Verificamos que onlyLines es un único número entero
    if (onlyLines != null && !onlyLines.isEmpty() && !onlyLines.matches("\\d+")) {
      throw new Exception("onlyLines no es un único número entero.");
    }

    String[] onlyFiles = null;
    if(onlyFilesString != null && !onlyFilesString.isEmpty()){
      onlyFiles = onlyFilesString.split("\\s+");
    }
    String[] notFiles = null;
    if(notFilesString != null && !notFilesString.isEmpty()){
      notFiles = notFilesString.split("\\s+");
    }
    int nlines = -1;
    if(onlyLines != null && !onlyLines.isEmpty()){
      nlines = Integer.parseInt(onlyLines);
    }

    Date start = new Date();
    try {
      System.out.println("Indexing to directory '" + indexPath + "'...");

      Directory dir = FSDirectory.open(Paths.get(indexPath));
      Analyzer analyzer = new StandardAnalyzer();
      IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

      if (create) {
        // Create a new index in the directory, removing any
        // previously indexed documents:
        iwc.setOpenMode(OpenMode.CREATE);
      } else {
        // Add new documents to an existing index:
        iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
      }

      // Optional: for better indexing performance, if you
      // are indexing many documents, increase the RAM
      // buffer.  But if you do this, increase the max heap
      // size to the JVM (eg add -Xmx512m or -Xmx1g):
      //
      // iwc.setRAMBufferSizeMB(256.0);

      KnnVectorDict vectorDictInstance = null;
      long vectorDictSize = 0;
      if (vectorDictSource != null) {
        KnnVectorDict.build(Paths.get(vectorDictSource), dir, KNN_DICT);
        vectorDictInstance = new KnnVectorDict(dir, KNN_DICT);
        vectorDictSize = vectorDictInstance.ramBytesUsed();
      }

      List<Directory> dirList;
      try (IndexFiles indexFiles = new IndexFiles(vectorDictInstance)) {
        indexFiles.createThreads(docDir, numThreads, create, depth, contentsStored, contentsTermVectors, indexFiles, onlyFiles, notFiles, nlines);
        dirList = indexFiles.getDirList();
        // NOTE: if you want to maximize search performance,
        // you can optionally call forceMerge here.  This can be
        // a terribly costly operation, so generally it's only
        // worth it when your index is relatively static (ie
        // you're done adding documents to it):
        //
        // writer.forceMerge(1);
      } finally {
        IOUtils.close(vectorDictInstance);
      }
      IndexWriter writer = null;
      try {
        writer = new IndexWriter(dir, iwc);
      } catch (IOException e) {
        e.printStackTrace();
      }

      Directory[] dirs = new Directory[dirList.size()];
      dirs = dirList.toArray(dirs);
      try {
        assert writer != null;
        writer.addIndexes(dirs);
      } catch (IOException e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }
      try {
        for(Directory d: dirs){
          d.close();
        }
        writer.commit();
        writer.close();
      } catch (IOException e1) {
        e1.printStackTrace();
      }

      Date end = new Date();
      try (IndexReader reader = DirectoryReader.open(dir)) {
        System.out.println(
            "Indexed "
                + reader.numDocs()
                + " documents in "
                + (end.getTime() - start.getTime())
                + " milliseconds");
        if (reader.numDocs() > 200
            && vectorDictSize < 1_000_000
            && System.getProperty("smoketester") == null) {
          throw new RuntimeException(
              "Are you (ab)using the toy vector dictionary? See the package javadocs to understand why you got this exception.");
        }
      }

    } catch (IOException e) {
      System.out.println(" caught a " + e.getClass() + "\n with message: " + e.getMessage());
    }
  }

  void createThreads(Path path, int numThreads, final Boolean create, final int depth, final boolean contentsStored, final boolean contentsTermVectors, final IndexFiles indexFiles,  String[] onlyFiles, String[] notFiles, int nlines) {

    final ExecutorService executor = Executors.newFixedThreadPool(numThreads);

    try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(path)) {

      /* We process each subfolder in a new thread. */
      for (final Path p : directoryStream) {
        if (Files.isDirectory(p)) {
          final Runnable worker = new WorkerThread(p, create, depth, contentsStored, contentsTermVectors, indexFiles, onlyFiles, notFiles, nlines);
          /*
           * Send the thread to the ThreadPool. It will be processed eventually.
           */
          executor.execute(worker);
        }
      }

    } catch (final IOException e) {
      e.printStackTrace();
      System.exit(-1);
    }

    /*
     * Close the ThreadPool; no more jobs will be accepted, but all the previously
     * submitted jobs will be processed.
     */
    executor.shutdown();

    /* Wait up to 1 hour to finish all the previously submitted jobs */
    try {
      executor.awaitTermination(1, TimeUnit.HOURS);
    } catch (final InterruptedException e) {
      e.printStackTrace();
      System.exit(-2);
    }

    System.out.println("Finished all threads");

  }

  /**
   * Indexes the given file using the given writer, or if a directory is given, recurses over files
   * and directories found under the given directory.
   *
   * <p>NOTE: This method indexes one document per input file. This is slow. For good throughput,
   * put multiple documents into your input file(s). An example of this is in the benchmark module,
   * which can create "line doc" files, one document per line, using the <a
   * href="../../../../../contrib-benchmark/org/apache/lucene/benchmark/byTask/tasks/WriteLineDocTask.html"
   * >WriteLineDocTask</a>.
   *
   * @param writer Writer to the index where the given file/dir info will be stored
   * @param path The file to index, or the directory to recurse into to find files to index
   * @throws IOException If there is a low-level I/O error
   */

  void indexDocs(final IndexWriter writer, Path path, int maxDepth, boolean contentsStored, boolean contentsTermVectors, String[] onlyFiles, String[] notFiles, int nlines) throws Exception {

    if (Files.isDirectory(path)) {
      Files.walkFileTree(
              path,
              new SimpleFileVisitor<>() {
                int currentDepth = 0;

                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                  if (currentDepth == maxDepth) {
                    return FileVisitResult.SKIP_SUBTREE;
                  }
                  currentDepth++;
                  return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                  String fileName = file.getFileName().toString();
                  if(notFiles != null){
                    for (String s: notFiles) {
                      if(fileName.endsWith(s)){
                        return FileVisitResult.CONTINUE;
                      }
                    }
                  }else if(onlyFiles != null){
                    boolean b = false;
                    for (String s: onlyFiles) {
                      if(fileName.endsWith(s)){
                        b = true;
                      }
                    }
                    if(!b){
                      return FileVisitResult.CONTINUE;
                    }
                  }
                  try {
                    indexDoc(writer, file, attrs.lastModifiedTime().toMillis(), contentsStored, contentsTermVectors, nlines);
                  } catch (
                          @SuppressWarnings("unused")
                                  IOException ignore) {
                    ignore.printStackTrace(System.err);
                    // don't index files that can't be read.
                  }
                  return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
                  currentDepth--;
                  return FileVisitResult.CONTINUE;
                }
              });
    } else {
      indexDoc(writer, path, Files.getLastModifiedTime(path).toMillis(), contentsStored, contentsTermVectors, nlines);
    }
  }

  /** Indexes a single document */
  void indexDoc(IndexWriter writer, Path file, long lastModified, boolean contentsStored, boolean contentsTermVectors, int onlyLines) throws IOException {
    try (InputStream stream = Files.newInputStream(file)) {

      // make a new, empty document
      Document doc = new Document();

      // Add the path of the file as a field named "path".  Use a
      // field that is indexed (i.e. searchable), but don't tokenize
      // the field into separate words and don't index term frequency
      // or positional information:
      Field pathField = new StringField("path", file.toString(), Field.Store.YES);
      doc.add(pathField);

      // Add the last modified date of the file a field named "modified".
      // Use a LongPoint that is indexed (i.e. efficiently filterable with
      // PointRangeQuery).  This indexes to milli-second resolution, which
      // is often too fine.  You could instead create a number based on
      // year/month/day/hour/minutes/seconds, down the resolution you require.
      // For example the long value 2011021714 would mean
      // February 17, 2011, 2-3 PM.
      doc.add(new LongPoint("modified", lastModified));

      // Add the contents of the file to a field named "contents".  Specify a Reader,
      // so that the text of the file is tokenized and indexed, but not stored.
      // Note that FileReader expects the file to be in UTF-8 encoding.
      // If that's not the case searching for special characters will fail.
      BufferedReader br = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
      StringBuilder sb = new StringBuilder();
      String line;
      int cont = 0;
      while ((line = br.readLine()) != null) {
        if(cont == onlyLines){
          break;
        }
        sb.append(line);
        sb.append(System.lineSeparator());
        ++cont;
      }

      FIELD_TYPE.setStored(contentsStored);
      FIELD_TYPE.setStoreTermVectors(contentsTermVectors);
      FIELD_TYPE.setStoreTermVectorPositions(contentsTermVectors);

      doc.add(new Field("contents", sb.toString(), FIELD_TYPE));

      if(!contentsStored){
        String hash = sha256(sb.toString());
        doc.add(
                new StringField("hash", hash, Field.Store.YES)
        );
      }

      doc.add(
              new StringField("hostname", InetAddress.getLocalHost().getHostName(), Field.Store.YES)
              );

      doc.add(
              new StringField("thread", Thread.currentThread().getName(), Field.Store.YES)
      );

      String type;
      if(Files.isSymbolicLink(file)){
        type = "symbolic link";
      }else if(Files.isRegularFile(file)){
        type = "regular file";
      }else if(Files.isDirectory(file)){
        type = "directory";
      }else {
        type = "other";
      }

      doc.add(
              new StringField("type", type, Field.Store.YES)
      );

      long sizeBytes = 0;
      try {
        sizeBytes = Files.size(file);
      } catch (IOException e) {
        e.printStackTrace();
      }

      //Bytes a KB
      double sizeKB = (double) sizeBytes / 1024;

      //Agregar campo sizeKB al documento e indexarlo
      Field sizeField = new DoublePoint("sizeKB", sizeKB);
      doc.add(sizeField);

      //Agregar campo sizeKB como stored
      StoredField storedSizeField = new StoredField("sizeKB", sizeKB);
      doc.add(storedSizeField);

      BasicFileAttributes attr = Files.readAttributes(file, BasicFileAttributes.class);
      FileTime creationTime = attr.creationTime();
      FileTime lastAccessTime = attr.lastAccessTime();
      FileTime lastModifiedTime = attr.lastModifiedTime();

      Date creationTimeDate = new Date(creationTime.toMillis());
      Date lastAccessTimeDate = new Date(lastAccessTime.toMillis());
      Date lastModifiedTimeDate = new Date(lastModifiedTime.toMillis());

      doc.add(
              new StringField("creationTime", creationTime.toString(), Field.Store.YES)
      );

      doc.add(
              new StringField("lastAccessTime", lastAccessTime.toString(), Field.Store.YES)
      );

      doc.add(
              new StringField("lastModifiedTime", lastModifiedTimeDate.toString(), Field.Store.YES)
      );

      doc.add(
              new StringField("creationTimeLucene", DateTools.dateToString(creationTimeDate, DateTools.Resolution.MILLISECOND), Field.Store.YES)
      );

      doc.add(
              new StringField("lastAccessTimeLucene", DateTools.dateToString(lastAccessTimeDate, DateTools.Resolution.MILLISECOND), Field.Store.YES)
      );

      doc.add(
              new StringField("lastModifiedTimeLucene", DateTools.dateToString(lastModifiedTimeDate, DateTools.Resolution.MILLISECOND), Field.Store.YES)
      );

      if (demoEmbeddings != null) {
        try (InputStream in = Files.newInputStream(file)) {
          float[] vector =
              demoEmbeddings.computeEmbedding(
                  new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8)));
          doc.add(
              new KnnVectorField("contents-vector", vector, VectorSimilarityFunction.DOT_PRODUCT));
        }
      }

      if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
        // New index, so we just add the document (no old document can be there):
        System.out.println("adding " + file);
        writer.addDocument(doc);
      } else {
        // Existing index (an old copy of this document may have been indexed) so
        // we use updateDocument instead to replace the old one matching the exact
        // path, if present:
        System.out.println("updating " + file);
        writer.updateDocument(new Term("path", file.toString()), doc);
      }
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(vectorDict);
  }
}

