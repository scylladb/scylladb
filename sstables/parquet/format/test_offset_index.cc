/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Verify the OffsetIndex we emit locates rows correctly.
#include "format/parquet_reader.hh"
#include "format/parquet_metadata.hh"
#include <cstdio>
#include <fstream>
using namespace sstables::parquet::format;
static std::vector<uint8_t> slurp(const char* p){std::ifstream f(p,std::ios::binary);
  return std::vector<uint8_t>((std::istreambuf_iterator<char>(f)),std::istreambuf_iterator<char>());}
int main(int argc,char**argv){
  int bad=0;
  for(int a=1;a<argc;++a){
    auto img=slurp(argv[a]); auto md=parse_footer(img);
    const auto& rg=md.row_groups[0];
    auto oi=parse_offset_index(img,rg.columns[0]);
    if(!oi){std::printf("FAIL %s: no OffsetIndex\n",argv[a]);++bad;continue;}
    // first page must start at row 0, and first_row_index must be strictly increasing
    bool ok = !oi->pages.empty() && oi->pages[0].first_row_index==0;
    for(size_t i=1;i<oi->pages.size();++i)
      if(oi->pages[i].first_row_index<=oi->pages[i-1].first_row_index) ok=false;
    // every page offset must land inside the file and on a real page header
    for(auto&pl:oi->pages)
      if(pl.offset<=0||size_t(pl.offset)>=img.size()||pl.compressed_page_size<=0) ok=false;
    // row lookup must land in the right page
    const int64_t n=rg.num_rows;
    for(int64_t row : {int64_t(0), n/3, n/2, n-1}){
      size_t pi=oi->page_for_row(row);
      if(pi>=oi->pages.size()){ok=false;break;}
      const int64_t lo=oi->pages[pi].first_row_index;
      const int64_t hi=(pi+1<oi->pages.size())?oi->pages[pi+1].first_row_index:n;
      if(row<lo||row>=hi){std::printf("  row %lld -> page %zu [%lld,%lld) WRONG\n",
        (long long)row,pi,(long long)lo,(long long)hi);ok=false;}
    }
    std::printf("%s %-28s pages=%-4zu rows=%-7lld\n",ok?"PASS":"FAIL",
      std::string(argv[a]).substr(std::string(argv[a]).rfind('/')+1).c_str(),
      oi->pages.size(),(long long)rg.num_rows);
    if(!ok)++bad;
  }
  std::printf("%s\n",bad?"OFFSET INDEX FAIL":"OFFSET INDEX PASS");
  return bad?1:0;
}
