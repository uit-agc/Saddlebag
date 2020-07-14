#ifndef DATAOBJECT_H
#define DATAOBJECT_H

#include <vector>
#include <iostream>
#include "message.cpp"

/* See Appendix A in thesis for usage, and Appendix B for API reference */

namespace saddlebags
{

template<typename TableKey_T, typename ItemKey_T, typename Msg_T> class Worker;

template<class TableKey_T, class ItemKey_T, class Msg_T>
class Item {
    public:
    Msg_T value;
    TableKey_T myTableKey;
    ItemKey_T myItemKey;
    Worker<TableKey_T, ItemKey_T, Msg_T> *worker = nullptr;
    int next_seqnum = 0;

    /**
     *
     */
    Item() {
    }

    Item (TableKey_T myTableKey, ItemKey_T myItemKey) {
        this->myTableKey = myTableKey;
        this->myItemKey = myItemKey;
    }

    /**
     *
     */
    void push(TableKey_T destTableKey = TableKey_T(),
        ItemKey_T destItemKey = ItemKey_T(),
        Msg_T val = Msg_T()) {

        Message<TableKey_T, ItemKey_T, Msg_T> msg;
        msg.dest_table = destTableKey;
        msg.dest_item = destItemKey;
        msg.src_table = myTableKey;
        msg.src_item = myItemKey;
        msg.value = val;

        assert(msg.src_table < this->worker->total_tables );
        assert(msg.dest_table < this->worker->total_tables);
        worker->enqueue_push_request(msg);

        if (SADDLEBAG_DEBUG > 5) {
            std::cout << "[Rank " << upcxx::rank_me() << "]"
                      << " Submitting for push with value " << val << ","
                      << " destined for Item " << msg.dest_item << ","
                      << " located on Rank " << worker->get_partition(msg.dest_table, msg.dest_item) << "."
                      << std::endl;
        }
    }

    /**
     *
     */
    void broadcast(TableKey_T destTableKey, ItemKey_T destItemKey, Msg_T val) {
        Message<TableKey_T, ItemKey_T, Msg_T> msg;
        msg.dest_table = destTableKey;
        msg.dest_item = destItemKey;
        msg.src_table = myTableKey;
        msg.src_item = myItemKey;
        msg.value = val;

        // TODO [Enhancement]: Implement broadcast
        // worker->tables[myTableKey]->broadcast_value = val;
        // worker->tables[myTableKey]->broadcast_origin_item = myItemKey;
    }

    /**
     * Called when the object is created AND when the table container attempts to create an identical object
     */
    virtual void refresh() {
    }

    /**
     * Called when the object is created. Only called once.
     */
    virtual void on_create() {
    }

    /*
     * Called when some message is pushed to this object
     */
    virtual void on_push_recv(Msg_T val) {
    }

    //Called when something is pulled from this object
    virtual Msg_T foreign_pull(int tag) {
        return value;
    }

    //Called when a pull originating at this object completes
    virtual void returning_pull(Message<TableKey_T, ItemKey_T, Msg_T> const & returning_message) {
    }

    //Called once per cycle, after communication is received
    virtual void before_work() {
    }

    //Called once per cycle, before communication is sent
    virtual void do_work() {
    }

    //Called at the end of cycle
    virtual void finishing_work() {
    }
};
}//end namespace
#endif
