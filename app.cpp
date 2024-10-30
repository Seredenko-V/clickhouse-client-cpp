#include "responses_handler.hpp"

#include <clickhouse/client.h>

#include <chrono>
#include <iostream>
#include <thread>

using namespace clickhouse;
using namespace std;

::clickhouse::Progress& operator+=( ::clickhouse::Progress& lhs, const ::clickhouse::Progress& rhs )
{
   lhs.rows += rhs.rows;
   lhs.bytes += rhs.bytes;
   lhs.total_rows += rhs.total_rows;
   lhs.written_rows += rhs.written_rows;
   lhs.written_bytes += rhs.written_bytes;
   return lhs;
}

void OnProgress( const ::clickhouse::Progress& value )
{
   cout << "[clickhouse] обработано "sv << value.written_rows << " строк из "sv << value.rows << endl;
   cout << "[clickhouse] обработано "sv << value.written_bytes << " байт из "sv << value.bytes << endl;
}

void OnData( const Block& block )
{
   cout << "columns = "s << block.GetColumnCount() << '\t' << "rows"s << block.GetRowCount() << endl;
}

void MyTest()
{
   Client client( ClientOptions().SetHost( "localhost" ) );
   client.Execute(
      "CREATE TABLE IF NOT EXISTS default.numbers (id UInt64, name String, date Date, test Nullable(UInt8)) ENGINE = "
      "Memory" );
   Query query( "DESCRIBE TABLE default.numbers"s );
   query.OnProgress( OnProgress );
   query.OnData(
      []( const Block& block )
      {
         for( size_t i = 0; i < block.GetRowCount(); ++i )
         {
            std::cout << block[ 0 ]->As< ColumnString >()->At( i ) << endl
                      << block[ 1 ]->As< ColumnString >()->At( i ) << endl
                      << block[ 2 ]->As< ColumnString >()->At( i ) << endl;
         }
      } );
   client.Select( query );

   {
      Block block;

      auto id = std::make_shared< ColumnUInt64 >();
      id->Append( 1 );

      auto name = std::make_shared< ColumnString >();
      name->Append( "one" );

      auto date = std::make_shared< ColumnDate >();
      date->Append( 86400 );

      auto test =
         std::make_shared< ColumnNullable >( std::make_shared< ColumnUInt8 >( vector< uint8_t >( { 0 } ) ),
                                             std::make_shared< ColumnUInt8 >( vector< uint8_t >( { false } ) ) );
      //      test->Append( true );

      block.AppendColumn( "id", id );
      block.AppendColumn( "name", name );
      block.AppendColumn( "date", date );
      block.AppendColumn( "test", test );

      client.Insert( "default.numbers", block );
   }

   client.Execute( "DROP TABLE default.numbers" );
}

void BigInsert( Client& client )
{
   auto id = std::make_shared< ColumnUInt64 >();
   auto name = std::make_shared< ColumnString >();
   auto date = std::make_shared< ColumnDate >();
   auto dec = std::make_shared< ColumnDecimal >( 3, 2 );
   constexpr int size = 5;
   for( int i = 0; i < size; ++i )
   {
      id->Append( i );
      name->Append( to_string( i ) );
      date->Append( time_t( i * 86400 ) );
      dec->Append( "123.45"s );
   }
   Block block;
   block.AppendColumn( "id", id );
   block.AppendColumn( "name", name );
   block.AppendColumn( "date", date );
   block.AppendColumn( "dec", dec );
   client.Insert( "default.numbers", block );
}

void SelectAll( Client& client )
{
   ResponsesHandler handler;
   Query query( "SELECT * FROM default.numbers"s );
   query.OnException( std::bind( &ResponsesHandler::ExceptionCallback, &handler, std::placeholders::_1 ) );
   query.OnProgress( std::bind( &ResponsesHandler::ProgressCallback, &handler, std::placeholders::_1 ) );
   query.OnData( std::bind( &ResponsesHandler::SelectCallback, &handler, std::placeholders::_1 ) );
   query.OnDataCancelable( std::bind( &ResponsesHandler::SelectCancelableCallback, &handler, std::placeholders::_1 ) );
   query.OnServerLog( std::bind( &ResponsesHandler::SelectServerLogCallback, &handler, std::placeholders::_1 ) );
   query.OnProfileEvents( std::bind( &ResponsesHandler::ProfileEventsCallback, &handler, std::placeholders::_1 ) );
   query.OnProfile( std::bind( &ResponsesHandler::ProfileCallbak, &handler, std::placeholders::_1 ) );
   //   client.Select( query );
   client.SelectCancelable( "SELECT * FROM default.numbers"s,
                            std::bind( &ResponsesHandler::SelectCancelableCallback, &handler, std::placeholders::_1 ) );
   handler.Pending();
}

void TestBigData()
{
   Client client( ClientOptions().SetHost( "localhost" ) );
   Query query(
      "CREATE TABLE IF NOT EXISTS default.numbers (id UInt64, name String, date Date, dec Decimal(3,2), "
      "test_null Nullable(Int8)) ENGINE = Memory" );
   client.Execute( query );
   BigInsert( client );
   SelectAll( client );
   client.Execute( "DROP TABLE default.numbers" );
   cout << endl;
}

void TestColumnUInt64()
{
   auto column = std::make_shared< ColumnUInt64 >();
   column->Append( 10 );
   Block block;
   block.AppendColumn( "value", column );
   map< string, Block > blocks;
   blocks.emplace( "test"s, block );

   ResponsesHandler handler;
   Query query( "SELECT value FROM test"s );
   query.OnData( std::bind( &ResponsesHandler::SelectCallback, &handler, std::placeholders::_1 ) );

   Client client( ClientOptions().SetHost( "localhost" ) );
   client.Execute( query, blocks );

   handler.Pending();
   const std::vector< clickhouse::Block >& result = handler.GetResult();
   for( size_t row = 0; row < result[ 0 ].GetRowCount(); ++row )
      cout << result[ 0 ][ 0 ]->As< ColumnUInt64 >()->At( row ) << endl;
   cout << "TestColumnUInt64 has been passed"s << endl;
}

void TestColumnString()
{
   auto column = std::make_shared< ColumnString >();
   column->Append( "first"s );
   column->Append( "second"s );
   column->Append( "third"s );
   Block block;
   block.AppendColumn( "text", column );
   map< string, Block > blocks;
   blocks.emplace( "test"s, block );

   ResponsesHandler handler;
   Query query( "SELECT text FROM test"s );
   query.OnData( std::bind( &ResponsesHandler::SelectCallback, &handler, std::placeholders::_1 ) );

   Client client( ClientOptions().SetHost( "localhost" ) );
   client.Execute( query, blocks );

   handler.Pending();
   const std::vector< clickhouse::Block >& result = handler.GetResult();
   for( size_t row = 0; row < result[ 0 ].GetRowCount(); ++row )
      cout << result[ 0 ][ 0 ]->As< ColumnString >()->At( row ) << endl;
   cout << "TestColumnString has been passed"s << endl;
}

void TestSeveralBlocks()
{
   map< string, Block > blocks;
   {
      auto id = std::make_shared< ColumnUInt64 >();
      id->Append( 0 );
      id->Append( 1 );
      auto column = std::make_shared< ColumnUInt64 >();
      column->Append( 42 );
      column->Append( 43 );

      Block block;
      block.AppendColumn( "id", id );
      block.AppendColumn( "value", column );
      blocks.emplace( "digit"s, block );
   }
   {
      auto id = std::make_shared< ColumnUInt64 >();
      id->Append( 0 );
      id->Append( 1 );
      auto column = std::make_shared< ColumnString >();
      column->Append( "history"s );
      column->Append( "clickhouse"s );

      Block block;
      block.AppendColumn( "id", id );
      block.AppendColumn( "text", column );
      blocks.emplace( "string"s, block );
   }

   ResponsesHandler handler;
   Query query( "SELECT * FROM string INNER JOIN digit ON digit.id = string.id"s );
   query.OnData( std::bind( &ResponsesHandler::SelectCallback, &handler, std::placeholders::_1 ) );

   Client client( ClientOptions().SetHost( "localhost" ) );
   client.Execute( query, blocks );

   handler.Pending();
   const std::vector< clickhouse::Block >& result = handler.GetResult();
   for( size_t row = 0; row < result[ 0 ].GetRowCount(); ++row )
   {
      cout << result[ 0 ][ 0 ]->As< ColumnUInt64 >()->At( row ) << '\t';
      cout << result[ 0 ][ 1 ]->As< ColumnString >()->At( row ) << '\t';
      cout << result[ 0 ][ 2 ]->As< ColumnUInt64 >()->At( row ) << '\t';
      cout << result[ 0 ][ 3 ]->As< ColumnUInt64 >()->At( row ) << endl;
   }
   cout << "TestSeveralBlocks has been passed"s << endl;
}

void TestScalar()
{
   ResponsesHandler handler;
   Query query( "SELECT $1 AS test"s );
   query.OnData( std::bind( &ResponsesHandler::SelectCallback, &handler, std::placeholders::_1 ) );

   auto value = std::make_shared< ColumnFloat64 >();
   value->Append( 3.14 );
   Block block;
   block.AppendColumn( "column"s, value );
   map< string, Block > blocks{ { "$1"s, block } };

   Client client( ClientOptions().SetHost( "localhost" ) );
   client.Execute( query, blocks, true );
   handler.Pending();
   const std::vector< clickhouse::Block >& result = handler.GetResult();

   cout << result[ 0 ][ 0 ]->AsStrict< ColumnFloat64 >()->At( 0 ) << endl;
   cout << "TestScalar has been passed"s << endl;
}

void TestSelectNanAndInf()
{
   Block answer;
   auto handler = [ &answer ]( const Block& block )
   {
      if( block.GetColumnCount() > 0 && block.GetRowCount() > 0 )
         answer = block;
   };

   Query query( "SELECT 0/0 AS test"s );
   query.OnData( handler );

   Client client( ClientOptions().SetHost( "localhost" ) );
   client.Execute( query );
   const double result = answer[ 0 ]->AsStrict< ColumnFloat64 >()->At( 0 );
   cout << "double = "s << result << endl;
   cout << "string = "s << to_string( result ) << endl;
   cout << "from string = "s << stod( to_string( result ) ) << endl;
   cout << "is nan = "s << isnan( stod( to_string( result ) ) ) << endl;
   cout << "TestSelectNanAndInf has been passed"s << endl;
}

void TestPatchExternalTables()
{
   //    TestColumnUInt64();
   //    TestColumnString();
   //    TestSeveralBlocks();
   TestSelectNanAndInf();
}

void TestWithTotals()
{
   ResponsesHandler handler;
   //   auto handler = [ &answer ]( const Block& block )
   //   {
   //      if( block.GetColumnCount() > 0 && block.GetRowCount() > 0 )
   //         answer = block;
   //   };
   Client client( ClientOptions().SetHost( "localhost" ) );

   client.Execute( "DROP TABLE IF EXISTS test_data;"s );
   client.Execute( "CREATE TABLE IF NOT EXISTS test_data (id Int32, text_num String, elem Int32) ENGINE = Memory;"s );
   client.Execute(
      "INSERT INTO test_data (id, text_num, elem) VALUES (1, 'first', 1), (2, 'second', 2), (3, 'third', 3), (4, 'first', 10), (5, 'second', 20), (6, 'third', 30);"s );

   Query query( "SELECT text_num, SUM( elem ) AS total FROM test_data GROUP BY text_num WITH ROLLUP ORDER BY total;"s );
   query.OnData( std::bind( &ResponsesHandler::SelectCallback, &handler, std::placeholders::_1 ) );
   query.OnProfileEvents( std::bind( &ResponsesHandler::ProfileEventsCallback, &handler, std::placeholders::_1 ) );
   client.Execute( query );
   clickhouse::Block answer = handler.GetBlock();

   client.Execute( "DROP TABLE IF EXISTS test_data;"s );

   // Вывод основного результата запроса.
   cout << "Количество строк в основном результате = "s << answer.GetRowCount() << endl;
   for( int row = 0; row < answer.GetRowCount(); ++row )
   {
      cout << answer[ 0 ]->AsStrict< ColumnString >()->At( row ) << '\t';
      cout << answer[ 1 ]->AsStrict< ColumnInt64 >()->At( row ) << endl;
   }
   cout << "TestWithTotals has been passed"s << endl;
}

int main()
{
   TestWithTotals();
   return 0;
}
