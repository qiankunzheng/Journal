#include "RWJournal.h"
#include "NVMJournal.h"

RWJournal* RWJournal::create(string dev, string conf, BackStore* s, Finisher *f, int type)
{
    return new NVMJournal(dev, conf, s, f);
}
