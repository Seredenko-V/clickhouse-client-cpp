#pragma once

#include <clickhouse/client.h>
#include <clickhouse/query.h>

#include <vector>

class ResponsesHandler {
public:
    void Pending();

    void ExceptionCallback( const clickhouse::Exception& /*except*/ );

    void ProgressCallback( const clickhouse::Progress& progress );

    void SelectCallback( const clickhouse::Block& block );

    clickhouse::Block GetResult() const;

    bool SelectCancelableCallback( const clickhouse::Block& block );

    bool SelectServerLogCallback( const clickhouse::Block& block );

    bool ProfileEventsCallback( const clickhouse::Block& block );

    void ProfileCallbak( const clickhouse::Profile& profile );

private:
    bool was_finish_ = false;
    clickhouse::Block block_;
};
