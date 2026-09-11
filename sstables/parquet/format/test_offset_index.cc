/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Verify the OffsetIndex we emit locates rows correctly.
//
// Two layers of assertion. The index has to be internally consistent -- first_row_index
// starting at 0 and strictly increasing, offsets inside the file, page_for_row() landing in
// the right page. And it has to agree with the *pages themselves*: every PageLocation must
// point at a real page header whose header-plus-body size is exactly compressed_page_size,
// and the num_rows those headers carry must chain to exactly the first_row_index sequence.
// Without the second layer an index that was merely plausible -- right shape, wrong bytes --
// would pass.
#include "format/parquet_reader.hh"
#include "format/parquet_metadata.hh"
#include "format/page_header.hh"
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
    // ...and that header must describe exactly the page the index says is there: its own size
    // plus its body is the recorded compressed_page_size, and -- for V2 pages, which carry
    // num_rows -- the row counts chain to exactly the first_row_index sequence and sum to the
    // row group. (A V1 page has no row count in its header, so a foreign file written with V1
    // pages gets the size check only.)
    const int64_t n=rg.num_rows;
    int64_t row_at=0; bool chained=true;
    for(size_t i=0;ok&&i<oi->pages.size();++i){
      const auto& pl=oi->pages[i];
      size_t consumed=0;
      page_header ph;
      try{
        ph=parse_page_header(std::span<const uint8_t>(img).subspan(size_t(pl.offset)),consumed);
      }catch(const std::exception& e){
        std::printf("  page %zu at %lld: header does not parse: %s\n",i,(long long)pl.offset,e.what());
        ok=false;break;
      }
      if(ph.type!=page_type::data_page_v2&&ph.type!=page_type::data_page){
        std::printf("  page %zu at %lld: not a data page\n",i,(long long)pl.offset);ok=false;break;
      }
      if(int64_t(consumed)+ph.compressed_page_size!=pl.compressed_page_size){
        std::printf("  page %zu: header %zu + body %d != PageLocation size %d\n",i,consumed,
          ph.compressed_page_size,pl.compressed_page_size);ok=false;break;
      }
      if(!ph.v2){chained=false;continue;}
      if(chained&&pl.first_row_index!=row_at){
        std::printf("  page %zu: first_row_index %lld but the preceding pages hold %lld rows\n",i,
          (long long)pl.first_row_index,(long long)row_at);ok=false;break;
      }
      row_at+=ph.v2->num_rows;
    }
    if(ok&&chained&&row_at!=n){
      std::printf("  pages hold %lld rows, row group declares %lld\n",(long long)row_at,(long long)n);ok=false;
    }
    // row lookup must land in the right page
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
