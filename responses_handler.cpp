#include "responses_handler.hpp"

#include <chrono>
#include <condition_variable>
#include <iostream>
#include <mutex>

using namespace std;

void ResponsesHandler::Pending()
{
    static mutex mtx;
    static condition_variable query_status;

    unique_lock< mutex > guard( mtx );
    query_status.wait_for( guard, 1s, [ & ]() { return was_finish_; } );
    query_status.notify_one();

    if( was_finish_ ) {
        cout << "Finished" << endl;
    } else {
        cout << "Didn't finish" << endl;
    }
}

void ResponsesHandler::ExceptionCallback( const clickhouse::Exception& /*except*/ )
{
    cout << "ExceptionCallback" << endl;
}

void ResponsesHandler::ProgressCallback( const clickhouse::Progress& progress )
{
    cout << "ProgressCallback" << endl;
    cout << "bytes = " << progress.bytes << '\t' << "rows = " << progress.rows << endl;
}

void ResponsesHandler::SelectCallback( const clickhouse::Block& block )
{
    cout << "SelectCallback" << endl;
    cout << "rows = " << block.GetRowCount() << '\t' << "columns = " << block.GetColumnCount() << endl;
    was_finish_ = block.GetColumnCount() == 0;
    if( block.GetColumnCount() > 0 && block.GetRowCount() > 0 ) {
        block_ = block;
    }
}

clickhouse::Block ResponsesHandler::GetResult() const
{
    return block_;
}

bool ResponsesHandler::SelectCancelableCallback( const clickhouse::Block& block )
{
    cout << "SelectCancelableCallback" << endl;
    cout << "rows = " << block.GetRowCount() << '\t' << "columns = " << block.GetColumnCount() << endl;
    was_finish_ = block.GetRowCount() == 0 && block.GetColumnCount() == 0;
    return true;
}

bool ResponsesHandler::SelectServerLogCallback( const clickhouse::Block& block )
{
    cout << "SelectServerLogCallback" << endl;
    cout << "rows = " << block.GetRowCount() << '\t' << "columns = " << block.GetColumnCount() << endl;
    return true;
}

bool ResponsesHandler::ProfileEventsCallback( const clickhouse::Block& block )
{
    cout << "ProfileEventsCallback" << endl;
    cout << "rows = " << block.GetRowCount() << '\t' << "columns = " << block.GetColumnCount() << endl;
    return true;
}

void ResponsesHandler::ProfileCallbak( const clickhouse::Profile& profile )
{
    cout << "ProfileCallbak" << endl;
    cout << "bytes = " << profile.bytes << '\t' << "rows = " << profile.rows << endl;
}
