#include "responses_handler.hpp"

#include <clickhouse/client.h>

#include <iostream>
#include <map>
#include <string>
#include <vector>

using namespace clickhouse;
using namespace std;

void TestScalar() {
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
   const clickhouse::Block& result = handler.GetResult();

   cout << result[ 0 ]->AsStrict< ColumnFloat64 >()->At( 0 ) << endl;
   cout << "TestScalar has been passed"s << endl;
}

int main() {
   TestScalar();
   return 0;
}
