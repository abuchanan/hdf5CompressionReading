import hdf.hdf5lib.H5
import hdf.hdf5lib.HDF5Constants
import java.nio.file.{Paths, Files}
import java.io.File
import math._

object h5reader {
  val debug: Boolean = true
  
  def main(args: Array[String]): Unit = {
    /*System.setProperty("java.library.path",
			 System.getProperty("java.library.path") +
			 java.io.File.pathSeparator +
			 "/mnt/common/abuchan1/code/scala/hdf5/lib/")
    println("starting")*/
    
    //h5read("/mnt/common/abuchan1/data/testData.h5", "/DS1")
    
    testing("/mnt/common/abuchan1/data/testData.h5", "/DS1")
  }
  
  def testing(filename: String, dataset: String){
    var file_id     = H5.H5Fopen(filename, HDF5Constants.H5F_ACC_RDONLY, HDF5Constants.H5P_DEFAULT) // TODO: add try
    var dataset_id  = H5.H5Dopen(file_id, dataset);  // TODO: add try
    var dspace = H5.H5Dget_space(dataset_id)
    
    var x  = 0
    var dx = 4
    var y  = 0
    var dy = 8
    
    var hyper_id   = H5.H5Sselect_hyperslab(dspace, HDF5Constants.H5S_SELECT_SET, Array[Long](x,y), null, Array[Long](x + dx, y + dy), null)
    var memspace   = H5.H5Screate_simple(2, Array[Long](x+dx,y+dy), Array[Long](32,64)) 
    var dset_datas = Array.ofDim[Double]((dx*dy).toInt)
    
    //println(Array[Long](x + dx, y + dy).deep.mkString(", "))
    
    var dread_id = H5Dread(dataset_id, HDF5Constants.H5T_NATIVE_DOUBLE, memspace, dspace, HDF5Constants.H5P_DEFAULT, dset_datas)
    if(debug)println(dset_datas.deep.mkString(", "))
  }

  
  
  def H5Dread(dataset_id: Int, mem_type_id: Int, mem_space_id: Int, file_space_id: Int, xfer_plist_id: Int, buf: java.lang.Object){
    var dataSetSpace = H5.H5Dget_space(dataset_id)                 // TODO: add try
    var numberDims   = H5.H5Sget_simple_extent_ndims(dataSetSpace) // TODO: add try
    var dcpl_id      = H5.H5Dget_create_plist(dataset_id)          // TODO: add try
    val layout       = H5.H5Pget_layout(dcpl_id)                   // TODO: add try
    
    //TODO: add layout check before this statement
    var chunkSize    = new Array[Long](numberDims)
    var ndims        = H5.H5Pget_chunk(dcpl_id, numberDims, chunkSize)  // TODO: add try
    
    var dimSize    = new Array[Long](numberDims)
    var maxDimSize = new Array[Long](numberDims)
    ndims          = H5.H5Sget_simple_extent_dims(dataSetSpace, dimSize, maxDimSize)
    
    if(layout != HDF5Constants.H5D_CHUNKED){
      println("NOT CHUNKED!!!!!!!!")
    }
    
    //TODO: anticipate H5S_ALL
    var readStart = new Array[Long](numberDims)
    var readEnd   = new Array[Long](numberDims)
    H5.H5Sget_select_bounds(file_space_id, readStart, readEnd)
    
    
    
    var chunkStart = new Array[Long](numberDims)
    var chunkEnd   = new Array[Long](numberDims)
    for (dim <- 0 to numberDims - 1){
      //TODO: get it to work with H5Sget_select_elem_pointlist and H5Sget_select_elem_npoints 
      chunkStart(dim) = math.floor(readStart(dim)/chunkSize(dim)).toLong
      chunkEnd(dim)   = math.floor(readEnd(dim)  /chunkSize(dim)).toLong
      println("start: \t" + readStart(dim) + "/" + chunkSize(dim) + " = " + chunkStart(dim))
      println("end: \t" + readEnd(dim) + "/" + chunkSize(dim) + " = " + chunkEnd(dim))
    }
    
    //println("Chunk Start:")
    //println(chunkStart.deep.mkString(", "))
    //println("Chunk End:")
    //println(chunkEnd.deep.mkString(", "))
    
    
    //
    //  Initialize shared memory
    //
    if(!Files.exists(Paths.get("/dev/shm/chunks"))){
      // Chunks folder does not exist
      new java.io.File("/dev/shm/chunks").mkdirs
    }
    
    var file_id = 123456789.toString
    if(!Files.exists(Paths.get("/dev/shm/chunks/" + file_id))){
      // File folder does not exist
      new java.io.File("/dev/shm/chunks/" + file_id).mkdirs
    }
    
    var ds_id = 123456789.toString
    if(!Files.exists(Paths.get("/dev/shm/chunks/" + file_id + "/" + ds_id))){
      // dataset folder does not exist
      new java.io.File("/dev/shm/chunks/" + file_id + "/" + ds_id).mkdirs
    }
    
    //
    //  Read chunk
    //
    var chunkID = 0
    if(!Files.exists(Paths.get("/dev/shm/chunks/" + file_id + "/" + ds_id + "/" + chunkID))){
      // Chunk is not in memory 
      
    } else {
      // Chunk already loaded in memory
      
    }
    
    
    H5.H5Dread(dataset_id, mem_type_id, mem_space_id, file_space_id, xfer_plist_id, buf)
  }
  
  def h5read(filename: String, dataset: String){
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
