#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os
import logging
import struct
import binascii
import pprint
def setup_logging():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s %(message)s')
    console.setFormatter(formatter)
    logger.addHandler(console)
    return logger
logger = setup_logging()
#def read_in_chunks(file_object, chunk_size=1024):
#    while True:
#        data = file_object.read(chunk_size)
#        if not data:
#            break
#        yield data
def read_file_in_chunks(filename, chunk_size=1024):
    with open(filename, 'rb') as f:
        while True:
            data = f.read(chunk_size)
            if not data:
                break
            yield data
def get_page_type(id):
    '''
/** File page types (values of FIL_PAGE_TYPE) @{ */
#define FIL_PAGE_INDEX		17855	/*!< B-tree node */
#define FIL_PAGE_UNDO_LOG	2	/*!< Undo log page */
#define FIL_PAGE_INODE		3	/*!< Index node */
#define FIL_PAGE_IBUF_FREE_LIST	4	/*!< Insert buffer free list */
/* File page types introduced in MySQL/InnoDB 5.1.7 */
#define FIL_PAGE_TYPE_ALLOCATED	0	/*!< Freshly allocated page */
#define FIL_PAGE_IBUF_BITMAP	5	/*!< Insert buffer bitmap */
#define FIL_PAGE_TYPE_SYS	6	/*!< System page */
#define FIL_PAGE_TYPE_TRX_SYS	7	/*!< Transaction system data */
#define FIL_PAGE_TYPE_FSP_HDR	8	/*!< File space header */
#define FIL_PAGE_TYPE_XDES	9	/*!< Extent descriptor page */
#define FIL_PAGE_TYPE_BLOB	10	/*!< Uncompressed BLOB page */
#define FIL_PAGE_TYPE_ZBLOB	11	/*!< First compressed BLOB page */
#define FIL_PAGE_TYPE_ZBLOB2	12	/*!< Subsequent compressed BLOB page */
#define FIL_PAGE_TYPE_LAST	FIL_PAGE_TYPE_ZBLOB2
					/*!< Last page type */
/* @} */
    '''
    FIL_PAGE_TYPE_DICT = {
         17855:'INDEX'
        ,2    :'UNDO_LOG'
        ,3    :'INODE'
        ,4    :'IBUF_FREE_LIST'
        ,0    :'FREE(ALLOCATED)'
        ,5    :'IBUF_BITMAP'
        ,6    :'SYS'
        ,7    :'TRX_SYS'
        ,8    :'FSP_HDR'
        ,9    :'XDES'
        ,10   :'BLOB'
        ,11   :'ZBLOB'
        ,12   :'ZBLOB2'
    }
    return FIL_PAGE_TYPE_DICT[id]
def process_page(page):
    fmt = '>IIiiQHQI'
    #print struct.calcsize(fmt)
    fil_page_space, fil_page_offset, fil_page_prev, fil_page_next, fil_page_lsn, fil_page_type, fil_page_file_flush_lsn, fil_page_arch_log_no = struct.unpack(fmt, page[0:38])
    info = {
        "space":fil_page_space
        ,"offset":fil_page_offset
        ,"prev":fil_page_prev
        ,"next":fil_page_next
        ,"lsn":fil_page_lsn
        ,"type":fil_page_type
        ,"type-name":get_page_type(fil_page_type)
        ,"file_flush_lsn":fil_page_file_flush_lsn
        ,"arch_log_no":fil_page_arch_log_no
    }
    return info
    #print binascii.hexlify(fil_page_type)
def space_page_type_regions_pages(pages):
    print "start    end      count     page_type"
    fmt = "%-8d %-8d %-8d  %s"
    start = 0
    end = 0
    prev_page_type = "none"
    for i in range(len(pages)):
        page = pages[i]
        page_type = page["type-name"]
        if (i > 0):
            if (page_type == prev_page_type):
                end = i
            else:
                print fmt % (start, end, end-start+1, prev_page_type)
                start = i
                end = start
        prev_page_type = page_type
    print fmt % (start, end, end-start+1, prev_page_type)
def space_page_type_regions_file(filename, pagesize=16*1024):
    pages = []
    for page in read_file_in_chunks(filename, pagesize):
        pages.append(process_page(page))
    #pp = pprint.PrettyPrinter(indent=4)
    #pp.pprint(pages)
    space_page_type_regions_pages(pages)
def main():
    if len(sys.argv) > 1:
        space_page_type_regions_file(sys.argv[1])
if __name__ == "__main__":
    main()
#eof
