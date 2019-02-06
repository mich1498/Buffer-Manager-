/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

//constructor
BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table
  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
/**
*
*
**/

	for (FrameId i = 0; i < numBufs; i++) 
	{
		if (bufDescTable[i].valid)
		{
			if (bufDescTable[i].dirty)
			{
				bufDescTable[i].file->writePage(bufPool[i]); //write page into file if dirty
				bufDescTable[i].Clear();
			}
		}
	}
	delete[] bufPool;
	delete[] bufDescTable;
	delete hashTable;
}

void BufMgr::advanceClock()
{
	clockHand = (clockHand + 1) % numBufs; // advance clock to next frame in the buffer pool
}

void BufMgr::allocBuf(FrameId & frame) 
{

	bool allPinned = true;  // Checker for if the frames are pinned
	bool allocDone = false;	// Checker for allocation
	FrameId i = 0; // Keeps track of the amount of frames iterated through with max of numBufs

	/**
	 * If there is at least one frame that is not pinned, we know we can allocate a free frame. Exit the
	 * loop knowing that all the frames are pinned if we iterated through all the frames
	 */
	while (allPinned && i < numBufs)
	{
		if (bufDescTable[i].pinCnt == 0)
		{
			allPinned = false;
		}

		i++;
	}

	// Throws BufferExceededException if all pages are pinned
	if (allPinned)
	{
		BufferExceededException e;
		throw e;
	}

	/**
	 * Clock algorithm that manages each frame, starts off by advancing the clock hand to the next
	 * frame in the buffer pool, if it is not valid, we are done because it is not referenced before.
	 * If the refbit is true, set it to false and go back to the beginning (advance clockHand etc).
	 * If the refbit is not set and it is pinned, go back to the beginning of the algorithm. If the
	 * frame is not pinned, we prepare the frame for allocation by removing it from the hash table.
	 * If the frame is dirty, flush the page to disk and set the page. If not, just clear the frame
	 */
	while (!allocDone)
	{
		advanceClock();
		if (bufDescTable[clockHand].valid == false)
		{
			frame = clockHand; //set frame to clockHand 
			bufDescTable[clockHand].Clear();
			allocDone = true;
		}
		else
		{
			if (bufDescTable[clockHand].refbit == true)
			{
				bufDescTable[clockHand].refbit = false;
			}
			else
			{
				if (bufDescTable[clockHand].pinCnt == 0)
				{

					hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);

					if (bufDescTable[clockHand].dirty == true)
					{
						(bufDescTable[clockHand].file)->writePage(bufPool[clockHand]);
					}
						frame = clockHand;
						bufDescTable[clockHand].Clear();
						allocDone = true;
				}
			}
		}

	}
}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{

/**
 * check whether the page is already in the buffer pool by invoking the lookup() method,
 * throw HashNotFoundException
 */
	FrameId fnum;
	try
	{ //invoke lookup to check if the page is already in the pool; 
		hashTable->lookup(file, pageNo, fnum);
		// if the page is in the pool 
		bufDescTable[fnum].refbit = true;       // set the appropriate refbit 
		bufDescTable[fnum].pinCnt++;            // increment the pincount for the page 
		page = &bufPool[fnum];
	}
	catch (HashNotFoundException e1)
	{ // when the page is not in the Hashtable
		allocBuf(fnum);                         // call allocBuf() to allocate a buffer frame for the particular page 
		Page rPage = file->readPage(pageNo);    // we read the page from disk 
		bufPool[fnum] = rPage;                  // insert page into buffer pool
		hashTable->insert(file, pageNo, fnum);  // insert the page into the hashtable 
		bufDescTable[fnum].Set(file, pageNo);   // invoke set on the frame , sets BufDesc member variable 
		page = &bufPool[fnum];                  // return a pointer to the frame via the page parameter 
	}

}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
	FrameId fnum;
	bool checkPage = false; //* check  if page is in the hashTable 
	try
	{
		hashTable->lookup(file, pageNo, fnum); //* look for the page in the hashTable 
		checkPage = true;  //* Page is found in the table
	}
	catch (HashNotFoundException e2)
	{
		checkPage = false; //* Page is not found in the hashTable   
	}
	/*
	 *If checkPage is true, implies the page is found in the hashTable 
	 *We then check if the pincount is 0, and throw an exception if the pincount is 0 
	 *Else we decrement the pin count and check the dirty bit. 
	*/
	if (checkPage) 
	{
		if (bufDescTable[fnum].pinCnt == 0)
		{
			throw PageNotPinnedException("Pincount of the page is 0", bufDescTable[fnum].pageNo, fnum); //* if Pincount is 0 throw exception 
		}
		else
		{
			bufDescTable[fnum].pinCnt--;                   //* Decrement the pinCount 
			if (dirty == true) bufDescTable[fnum].dirty = true; //* if the dirty is true, set the dirty bit 
		}
	}
}

void BufMgr::flushFile(const File* file) 
{
	// Iterates through all the frames to flush all the pages associate with the file
	for (FrameId i = 0; i < numBufs; i++)
	{
		// If the filename and file pointer of the page matches, it is associated with the page
		if (bufDescTable[i].file == file && bufDescTable[i].file->filename() == file->filename())
		{
			// If the page is pinned, throw exception
			if (bufDescTable[i].pinCnt > 0)
			{
				throw PagePinnedException(file->filename(), bufDescTable[i].pageNo, bufDescTable[i].frameNo);
			}
			// If the page is dirty, write it to disk
			if (bufDescTable[i].dirty == true)
			{
				bufDescTable[i].file->writePage(bufPool[i]);
			}
			// Either way we need to remove it from metadata since we are flushing it
			hashTable->remove(file, bufDescTable[i].pageNo);
			// If the page is not valid, throw exception
			if (bufDescTable[i].valid == false)
			{
				throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
			}
			// Clean metadata
			bufDescTable[i].Clear();
		}
	}
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
	FrameId fnum;
	Page npage = file->allocatePage();                    //* Allocate an empty page in the specied file  by invoking file->allocatePage()
	allocBuf(fnum);                                       //* Call allocBuf to obtain a bufferPool Frame
	bufPool[fnum] = npage;                                //* insert the page into the buffer pool 
	hashTable->insert(file, npage.page_number(), fnum);   //* the entry is inserted into the hashtable 
	bufDescTable[fnum].Set(file, npage.page_number());
	page = &bufPool[fnum];                                //* return a pointer to the frame via the page parameter 
	pageNo = npage.page_number();                         //* return the page number of newly allocated page 
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
	/* We need to 2 things here
 *  1. check if the page is in the page is in the hash table
 *  2. if the page is in the hashtable, check if the page is pinned or unpinned
 *  3. if a page is pinned, we cannot dispose the page (i think )
*/
	FrameId fnum;
	bool check = false; //*a boolean variable to check if page can be disposed or not
	try
	{
		hashTable->lookup(file, PageNo, fnum);
		if (bufDescTable[fnum].pinCnt != 0)
		{
			throw PagePinnedException("Page  is currently Pinned ", PageNo, fnum);
			check = false; //* page cannot be disposed 
		}
		else check = true; //* page is in the hashtable and can be  disposed 

	}
	catch (BadgerDbException e3)
	{
		check = false; //* page cannot be disposed 
	}
	if (check == true)
	{
		hashTable->remove(file, PageNo); //* remove the entry from the hashtable 
		bufDescTable[fnum].Clear();       //*  free buffer frame i think , what else would clear do ? :P 
		file->deletePage(PageNo);

	}
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
