package org.hashbang.fs;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.FileSystem;

/**
 * @author Ankita
 * DataTransferUtil : A utility class to provide basic functionalities data transfer using file and streams.
 */

public class DataTransferUtil {


	private FileSystem fs;
	private int bufferCapacity;

	public DataTransferUtil(){
		fs = FileSystems.getDefault();
		bufferCapacity = -1;
	}

	public String getPathSeperator(){
		return fs.getSeparator();
	}

	/**
	 * readStreamBuffer : read data from inputStream to a buffer
	 * @param inputStream
	 * @return data
	 */
	public byte[] readStreamToBuffer(InputStream inputStream) {
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		try {
			byte[] data = new byte[1024];
			int bytesRead;
			while ((bytesRead = inputStream.read(data)) != -1) {
				outputStream.write(data, 0, bytesRead);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return outputStream.toByteArray();
	}

	/**
	 * readFileFromBuffer : Read file represented by path from given offset
	 * 					    upto the bufferCapacity into the Buffer
	 * @param p
	 * @param offset
	 * @return buffer containing the read bytes
	 */
	public byte[] readFileToBuffer(Path p, long offset, int size){
		if (!Files.exists(p)) {
			System.out.println("Invalid path :" + p.toAbsolutePath());
			return null;
		}

		byte[] data = null;
		try {
			RandomAccessFile file = new RandomAccessFile(p.toFile(), "r");
			if (size < 0) {
				size = (int) (file.length() - offset);
			}
			data = new byte[size];
			file.seek(offset);
			int nBytes = file.read(data);
			System.out.println("reading " + nBytes + " out of " + size + " bytes from offset: " + offset);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return data;
	}


	/**
	 * writeBufferToFile : write buffer to file specified by Path
	 * @param data
	 * @param fileName
	 */
	public void writeBufferToFile(byte[] data, String fileName){
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(fileName);
			fos.write(data);
			fos.close();
			System.out.println("Wrote " + data.length + " bytes to " + fileName);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * writeFromBufferToStream : writes buffer data to the output stream
	 * @param data
	 * @param out
	 */
	public void writeFromBufferToStream(byte[] data, OutputStream out){
		try {
			out.write(data);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}



	/**
	 * removeFile : delete file or dir specified by Path, isDir
	 * @param p
	 * @param recursive
	 * @param isDir
	 * @return true if file is deleted else false
	 */
	public boolean removeFile(Path p, boolean recursive, boolean isDir){
		if(verifyPath(p)){
			if(isDir){
				File[] contents = p.toFile().listFiles();
				if (contents != null) {
					for (File f : contents) {
						try {
							Files.deleteIfExists(f.toPath());
						} catch (IOException e) {
							System.out.println("remove : Exception in remove file "+f.getAbsolutePath());
							e.printStackTrace();
						}
					}
				}
				try {
					return Files.deleteIfExists(p);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}else{
				try {
					return Files.deleteIfExists(p);
				} catch (NoSuchFileException x) {
					System.err.format("%s: no such" + " file or directory%n", p);
				} catch (DirectoryNotEmptyException x) {
					System.err.format("%s not empty%n", p);
				} catch (IOException x) {
					System.err.println(x);
				}
			}
		}
		return false;
	}




	/**
	 * createFile : create a file/dir in the specified Path
	 * @param p
	 * @param fileName
	 * @param isDir
	 * @return
	 */
	public Path createFile(Path p, String fileName, boolean isDir){
		if(isDir){
			return mkdir(p, fileName);
		}else{
			if(verifyPath(p)){
				try {
					return Files.createFile(appendToPath(p, fileName));
				} catch (IOException e) {
					System.out.println("Exception in create file");
					e.printStackTrace();
				}
			}
		}
		return null;
	}



	/**
	 * mkdir : make dir @fileName in the parent specified by path
	 * @param p
	 * @param fileName
	 * @return
	 */
	public Path mkdir(Path p, String fileName){
		Path dirPath = null;
		try {
			if(verifyPath(p)){
				dirPath = appendToPath(p, fileName);
				return Files.createDirectory(dirPath);
			}
		} catch (IOException e) {
			System.out.println("Exception in mkdir");
			e.printStackTrace();
		}
		return dirPath;
	}

	/**
	 * mergeFiles : merge all files in the Path array into a file(outputFile)
	 * @param paths
	 * @param outputFile
	 * @return
	 */
	public Path mergeFiles(Path[] paths, Path outputFile){

		FileOutputStream fos = null;
		FileChannel outputFileChannel = null;
		try {
			fos = new FileOutputStream(outputFile.toFile());
			outputFileChannel = fos.getChannel();
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}

		FileInputStream fis = null;
		FileChannel inputFileChannel = null;
		for(Path p : paths){
			if(verifyPath(p)){
				try {
					fis = new FileInputStream(p.toFile());
					inputFileChannel = fis.getChannel();
					ByteBuffer bytebuf = ByteBuffer.allocateDirect(bufferCapacity);

					// Read data from file into ByteBuffer
					int bytesCount = 0;
					while ((bytesCount = inputFileChannel.read(bytebuf)) > 0) {
						// flip the buffer which set the limit to current position, and position to 0.
						bytebuf.flip();
						outputFileChannel.write(bytebuf);  // Write data from ByteBuffer to file
						bytebuf.clear();     // For the next read
					}
					try {
						fis.close();
						inputFileChannel.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				} catch (FileNotFoundException e) {

					e.printStackTrace();
				} catch (IOException e) {

					e.printStackTrace();
				}finally{
					try {
						if(outputFileChannel != null)
							outputFileChannel.close();
						if(fos != null)
							fos.close();
						if(fis != null)
							fis.close();
						if(inputFileChannel != null)
							inputFileChannel.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}else{
				System.out.println("mergeFiles : invalid path "+p.toAbsolutePath());
			}
		}
		return outputFile;
	}

	/**
	 * verifyPath : returns true if file with given path exist, does not consider symlink
	 * @param p
	 * @return returns true if exist else false
	 */
	public boolean verifyPath(Path p){
		return Files.exists(p) && Files.isReadable(p);
	}


	/**
	 * appendToPath : append a fileName or directory name to the given path
	 * @param p
	 * @param fileName
	 * @return the new path including the name of file
	 */
	public Path appendToPath(Path p , String fileName){
		if(verifyPath(p)){
			String path = p.toAbsolutePath().toString()+getPathSeperator()+fileName;
			return Paths.get(path);
		}
		return null;
	}

}
