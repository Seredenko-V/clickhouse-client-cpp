#pragma once

#include <clickhouse/client.h>
#include <clickhouse/query.h>

#include <chrono>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <vector>

using namespace std::chrono_literals;

class ResponsesHandler
{
public:
   void Pending()
   {
      static std::mutex mtx;
      static std::condition_variable query_status;

      // Взять параметр времени ожидания из облака 5 секунд по дефолту.
      std::unique_lock< std::mutex > guard( mtx );
      query_status.wait_for( guard, 1s, [ & ]() { return mWasFinish; } );
      query_status.notify_one();

      if( mWasFinish )
         std::cout << "Finished" << std::endl;
      else
         std::cout << "Didn't finish" << std::endl;
   }

   void ExceptionCallback( const clickhouse::Exception& /*except*/ )
   {
      std::cout << "ExceptionCallback" << std::endl;
   }

   void ProgressCallback( const clickhouse::Progress& progress )
   {
      std::cout << "ProgressCallback" << std::endl;
      std::cout << "bytes = " << progress.bytes << '\t' << "rows = " << progress.rows << std::endl;
   }

   void SelectCallback( const clickhouse::Block& block )
   {
      std::cout << "SelectCallback" << std::endl;
      std::cout << "rows = " << block.GetRowCount() << '\t' << "columns = " << block.GetColumnCount() << std::endl;
      mWasFinish = block.GetColumnCount() == 0;
      if( block.GetColumnCount() > 0 && block.GetRowCount() > 0 )
      {
         mBlocks.emplace_back( block );
         mBlock = block;
      }
   }

   const std::vector< clickhouse::Block >& GetResult() const
   {
      return mBlocks;
   }

   clickhouse::Block GetBlock() const
   {
      return mBlock;
   }

   bool SelectCancelableCallback( const clickhouse::Block& block )
   {
      std::cout << "SelectCancelableCallback" << std::endl;
      std::cout << "rows = " << block.GetRowCount() << '\t' << "columns = " << block.GetColumnCount() << std::endl;
      mWasFinish = block.GetRowCount() == 0 && block.GetColumnCount() == 0;
      return true;
   }

   bool SelectServerLogCallback( const clickhouse::Block& block )
   {
      std::cout << "SelectServerLogCallback" << std::endl;
      std::cout << "rows = " << block.GetRowCount() << '\t' << "columns = " << block.GetColumnCount() << std::endl;
      return true;
   }

   bool ProfileEventsCallback( const clickhouse::Block& block )
   {
      std::cout << "ProfileEventsCallback" << std::endl;
      std::cout << "rows = " << block.GetRowCount() << '\t' << "columns = " << block.GetColumnCount() << std::endl;

      return true;
   }

   void ProfileCallbak( const clickhouse::Profile& profile )
   {
      std::cout << "ProfileCallbak" << std::endl;
      std::cout << "bytes = " << profile.bytes << '\t' << "rows = " << profile.rows << std::endl;
   }

private:
   bool mWasFinish = false;
   std::vector< clickhouse::Block > mBlocks;
   clickhouse::Block mBlock;
};
