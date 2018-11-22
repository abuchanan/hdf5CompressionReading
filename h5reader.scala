import hdf.hdf5lib.H5
import hdf.hdf5lib.HDF5Constants
import java.nio.file.{Paths, Files}
import java.io.{BufferedWriter, FileWriter}
import java.io.File
import math._

object h5reader {
  val debug: Boolean = true
  
  def main(args: Array[String]): Unit = {
    // 
    //testtest("/mnt/common/abuchan1/data/testData.h5", "/DS1")
    

    
    readTester("/mnt/common/abuchan1/data/testData.h5", "/DS1")
  }
  
  def readTester(filename: String, dataset: String){
    //
    //  Open file and data set
    //
    var file_id     = H5.H5Fopen(filename, HDF5Constants.H5F_ACC_RDONLY, HDF5Constants.H5P_DEFAULT) // TODO: add try
    var dataset_id  = H5.H5Dopen(file_id, dataset);  // TODO: add try
    var dspace 	    = H5.H5Dget_space(dataset_id)
    
    // 
    //  Define area of data set to read
    //
    var x  = 7
    var dx = 2
    var y  = 7
    var dy = 4
    
    //
    //  Define hyperslab and memory space
    //
    var hyper_id   = H5.H5Sselect_hyperslab(dspace, HDF5Constants.H5S_SELECT_SET, Array[Long](x,y), null, Array[Long](x + dx, y + dy), null)
    var memspace   = H5.H5Screate_simple(2, Array[Long](x+dx,y+dy), Array[Long](32,64)) 
    var dset_datas = Array.ofDim[Double]((dx*dy).toInt + 1)
    //println(Array[Long](x + dx, y + dy).deep.mkString(", "))
    
    //
    // Perform the actual read
    //
    var dread_id = H5Dread(dataset_id, HDF5Constants.H5T_NATIVE_DOUBLE, memspace, dspace, HDF5Constants.H5P_DEFAULT, dset_datas)
    if(debug)println(dset_datas.deep.mkString(", "))
  }

  
  
  def H5Dread(dataset_id: Int, mem_type_id: Int, mem_space_id: Int, file_space_id: Int, xfer_plist_id: Int, buf: java.lang.Object){
    //
    //  Basic initialization stuff
    //
    var dataSetSpace = H5.H5Dget_space(dataset_id)                 // TODO: add try
    var numberDims   = H5.H5Sget_simple_extent_ndims(dataSetSpace) // TODO: add try
    var dcpl_id      = H5.H5Dget_create_plist(dataset_id)          // TODO: add try
    val layout       = H5.H5Pget_layout(dcpl_id)                   // TODO: add try
    
    //
    //  Check if data set is chunked
    //
    if(layout != HDF5Constants.H5D_CHUNKED){
      H5.H5Dread(dataset_id, mem_type_id, mem_space_id, file_space_id, xfer_plist_id, buf)
    }

    // Get chunk size and number of dimensions
    var chunkSize  = new Array[Long](numberDims)
    var ndims      = H5.H5Pget_chunk(dcpl_id, numberDims, chunkSize)  // TODO: add try
    var dimSize    = new Array[Long](numberDims)
    var maxDimSize = new Array[Long](numberDims)
    ndims          = H5.H5Sget_simple_extent_dims(dataSetSpace, dimSize, maxDimSize)
   
    //
    //  Find the read bounds of the hyper slab
    //
    //TODO: anticipate H5S_ALL
    var readStart = new Array[Long](numberDims)
    var readEnd   = new Array[Long](numberDims)
    H5.H5Sget_select_bounds(file_space_id, readStart, readEnd)
    
    
    //
    //  Find which chunks need to be read
    //
    var chunkStart = new Array[Long](numberDims)
    var chunkEnd   = new Array[Long](numberDims)
    for (dim <- 0 to numberDims - 1){
      //TODO: get it to work with H5Sget_select_elem_pointlist and H5Sget_select_elem_npoints 
      chunkStart(dim) = math.floor(readStart(dim)/chunkSize(dim)).toLong
      chunkEnd(dim)   = math.floor(readEnd(dim)  /chunkSize(dim)).toLong
      println("dim " + dim + ") start: \t" + readStart(dim) + "/" + chunkSize(dim) + " = " + chunkStart(dim))
      println("dim " + dim + ") end: \t" + readEnd(dim) + "/" + chunkSize(dim) + " = " + chunkEnd(dim))
    }
    
    println("Chunk Start:")
    println(chunkStart.deep.mkString(", "))
    println("Chunk End:")
    println(chunkEnd.deep.mkString(", "))
     

    val numberChunks = findNumberOfChunks(numberDims, chunkStart, chunkEnd)
    var chunks = Array.ofDim[Int](numberChunks, numberDims)
    getListOfChunks(numberDims, chunkStart, chunkEnd, numberChunks, chunks)
    //println("\n")
    //chunks foreach { row => row foreach print; println }
    //println("\n")

    //
    //  Initialize shared memory
    //
    var sharedDir: String = "/dev/shm/chunks"
    if(!Files.exists(Paths.get(sharedDir))){
      // Chunks folder does not exist
      new java.io.File(sharedDir).mkdirs
    }
    
    var file_id = 123456789.toString
    sharedDir   = sharedDir + "/" + file_id
    if(!Files.exists(Paths.get(sharedDir))){
      // File folder does not exist
      new java.io.File(sharedDir).mkdirs
    }
    
    var ds_id = 123456789.toString
    sharedDir = sharedDir + "/" + ds_id
    if(!Files.exists(Paths.get(sharedDir))){
      // dataset folder does not exist
      new java.io.File(sharedDir).mkdirs
    }
    
    //
    //  Read chunk
    // 
   loadAllChunks(chunks, chunkSize, sharedDir, dataset_id, ndims)
    
    
    H5.H5Dread(dataset_id, mem_type_id, mem_space_id, file_space_id, xfer_plist_id, buf)
  }

  //Load all chunks into memory first
  def loadAllChunks(chunks: Array[Array[Int]], chunkSize: Array[Long], dir: String, dataset_id: Int, ndims: Int){
    for(chunk <- chunks){
      val chunkID:String = chunk.deep.mkString("-")
      val curdir = dir + "/" + chunkID
      if(!Files.exists(Paths.get(curdir))){
        if(debug) println("NOT FOUND: " + curdir)
        loadChunk(curdir, chunk, chunkSize, dataset_id, ndims: Int)
      } 
    }
  }
  def loadChunk(filename: String, chunk: Array[Int], chunkSize: Array[Long], dataset_id: Int, ndims: Int){
    var dimSize = new Array[Long](ndims)
    var maxDimSize = new Array[Long](ndims) 
    var dspace = H5.H5Dget_space(dataset_id)
    H5.H5Sget_simple_extent_dims(dspace, dimSize, maxDimSize)

    var readStart: Array[Long] = new Array(ndims)
    var readEnd:   Array[Long] = new Array(ndims)
    var dataSize:  Int         = 1
    println("Reading chunk " + chunk.deep.mkString("-") + ": ")
    for (dim <- 0 to ndims-1){

      readStart(dim) = chunk(dim) * chunkSize(dim)
      readEnd(dim)   = readStart(dim) + chunkSize(dim) - 1 // might have to subtract 1
      dataSize = dataSize * (chunkSize(dim)).toInt
      println("\tdim " + dim + " starting " + readStart(dim) + " ending " + readEnd(dim))
    }

    // ask for datatype as input
    var hyper_id = H5.H5Sselect_hyperslab(dspace, HDF5Constants.H5S_SELECT_SET, readStart, null, readEnd, null)
    var memspace = H5.H5Screate_simple(ndims, readEnd, maxDimSize)
    var buf = new Array[Double](dataSize) // TODO: change to match input datatype or ask kun

    try{
      H5.H5Dread(dataset_id, HDF5Constants.H5T_NATIVE_DOUBLE, memspace, dspace, HDF5Constants.H5P_DEFAULT, buf) 
     
      val file = new File(filename)
      val bw = new BufferedWriter(new FileWriter(file))
      buf.foreach(entry => bw.write(entry + "\n"))
      bw.close()
    }
    catch{
      case e: Exception => println(e)
      case unknown : Throwable => println(unknown)
    }

    println(" ")

  }
  //Read all data from chunks not H5Dread
  def readData(dir: String, ndims: Int, chunkSize: Array[Int], start: Array[Long], end: Array[Long]){
    var numCells = 1
    var dim = 0
    var cell = 0
    for (dim  <- 0 to ndims-1){
      numCells = numCells * (end(dim) - start(dim) + 1).toInt
    }
    for (cell <- 0 to numCells-1){
      var cord = new Array[Long](ndims)
      getCord(ndims, start, end, cell, cord)
      println("cell " + cell + " = " + cord.deep.mkString(","))
    }
  }
  def getCord(ndims: Int, start: Array[Long], end: Array[Long], cell: Int, cord: Array[Long]){
    for (dim <- 0 to ndims-1){
      val dif = end(dim) - start(dim) + 1
      var divider = 1
      for (div <- 0 to dim-1){
        divider = divider * (end(div) - start(div) + 1).toInt
      }
      cord(dim) = (floor(cell/divider)%dif).toInt + start(dim).toInt
    }
  }

 
  def findNumberOfChunks(ndims: Int, start: Array[Long], end: Array[Long]): Int = {
    var nchunks: Int = 1
    var dim          = 0
    for (dim <- 0 to ndims-1){
      nchunks = nchunks * (end(dim).toInt - start(dim).toInt + 1).toInt
    }
    nchunks
  }

  def getListOfChunks(ndims: Int, start: Array[Long], end: Array[Long], nchunks: Int, chunks: Array[Array[Int]]){
    var chunk = 0
    var dim   = 0
    for (chunk <- 0 to nchunks-1){
      //var chunkID = new Array[Int](ndims)
      for (dim <- 0 to ndims-1){
        val diff: Int    = end(dim).toInt - start(dim).toInt + 1
        var divider: Int = 1
        var div          = 0
        for(div <- 0 to dim - 1){
          divider    = divider * (end(div) - start(div) + 1).toInt
        }
        //chunkID(dim) = (floor(chunk/divider)%diff).toInt
        chunks(chunk)(dim) = (floor(chunk/divider)%diff).toInt + start(dim).toInt
      }
      //println(chunkID.deep.mkString(", "))
      
    }
  }











 
  def getListOfChunksOld(ndims: Int, start: Array[Long], end: Array[Long]){
    var nchunks: Int = 1
    var dim          = 0
    for (dim <- 0 to ndims-1){
      nchunks = nchunks * (end(dim).toInt - start(dim).toInt + 1).toInt
    }
    var chunk = 0
    dim       = 0
    for (chunk <- 0 to nchunks-1){
      var chunkID = new Array[Int](ndims)
      for (dim <- 0 to ndims-1){
        val diff: Int    = end(dim).toInt - start(dim).toInt + 1
        var divider: Int = 1
        var div          = 0
        for(div <- 0 to dim - 1){
          divider    = divider * (end(div) - start(div) + 1).toInt
        }
        chunkID(dim) = (floor(chunk/divider)%diff).toInt
      }
     println(chunkID.deep.mkString(", "))
    } 
  }



  //
  //
  //
  //	IGNORE THIS TEST FUNCTION
  //
  //
  def testtest(filename: String, dataset: String){
    var file_id = -1
    var dataset_id = -1
    var dcpl_id = -1
    try {
      file_id = H5.H5Fopen(filename, HDF5Constants.H5F_ACC_RDONLY, HDF5Constants.H5P_DEFAULT) // TODO: add try
      dataset_id = H5.H5Dopen(file_id, dataset);  // TODO: add try
      dcpl_id = H5.H5Dget_create_plist(dataset_id)
    }
    catch{
      case e: Exception => println(e)
      case unknown : Throwable => println(unknown)
    }
    
    if(debug) println("opened file: \t\t" + file_id)
    if(debug) println("opened dataset: \t" + dataset_id)
    if(debug) println("opened property list:\t" + dcpl_id)
    
    if (dcpl_id >= 0) {
      if(debug){
        print("Dataset " + dataset_id + " has storage layout: ")
        printLayout(dcpl_id)
      }
    }
    
    var dspace = -1
    var ndims = -1
    dspace = H5.H5Dget_space(dataset_id)
    ndims = H5.H5Sget_simple_extent_ndims(dspace)
    var chunkSize = new Array[Long](ndims)
    var dimSize = new Array[Long](ndims)
    var maxDimSize = new Array[Long](ndims)
    
    if(debug)println()
    if(debug)print("H5P_get_chunk( " + dcpl_id + ", " + ndims + ", " + chunkSize + ") = ")
    if(debug)println(H5.H5Pget_chunk(dcpl_id, ndims, chunkSize))
    if(debug)println("chunkSize = " + chunkSize.deep.mkString(", "))
    
    if(debug)println()
    if(debug)print("H5Sget_simple_extent_dims( " + dspace + ", " + dimSize + ", " + maxDimSize + " ) = ")
    if(debug)println(H5.H5Sget_simple_extent_dims(dspace, dimSize, maxDimSize))
    if(debug)println("dimSize =\t" + dimSize.deep.mkString(", "))
    if(debug)println("maxDimSize =\t" + maxDimSize.deep.mkString(", "))
    if(debug)println()
    
    var x  = 0
    var dx = 4
    var y  = 0
    var dy = 8
    
    var hyper_id = H5.H5Sselect_hyperslab(dspace, HDF5Constants.H5S_SELECT_SET, Array[Long](x,y), null, Array[Long](x + dx, y + dy), null)
    var memspace = H5.H5Screate_simple(ndims, Array[Long](4,8), maxDimSize)
    
    //var subset_length:Long = 1
    //for (i <- 1 to ndims-1) {
    //  subset_length = subset_length * dimSize(i)
    //}
    var dset_datas = Array.ofDim[Double]((4*8).toInt)
    
    var dread_id = H5.H5Dread(dataset_id, HDF5Constants.H5T_NATIVE_DOUBLE, memspace, dspace, HDF5Constants.H5P_DEFAULT, dset_datas)
    if(debug)println(dset_datas.deep.mkString(", "))
  }
    
  
  def printLayout(layout: Int): Unit = H5.H5Pget_layout(layout) match{
    case HDF5Constants.H5D_COMPACT =>      println("H5D_COMPACT")
    case HDF5Constants.H5D_CONTIGUOUS =>   println("H5D_CONTIGUOUS")
    case HDF5Constants.H5D_CHUNKED =>      println("H5D_CHUNKED")
    case HDF5Constants.H5D_LAYOUT_ERROR => println("H5D_LAYOUT_ERROR")
    case HDF5Constants.H5D_NLAYOUTS =>     println("H5D_NLAYOUTS")
    case _ =>                              println("Unknown")
    
  }
}
