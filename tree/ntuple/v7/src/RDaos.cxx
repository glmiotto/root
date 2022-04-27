/// \file RDaos.cxx
/// \ingroup NTuple ROOT7
/// \author Javier Lopez-Gomez <j.lopez@cern.ch>
/// \date 2020-11-14
/// \warning This is part of the ROOT 7 prototype! It will change without notice. It might trigger earthquakes. Feedback
/// is welcome!

/*************************************************************************
 * Copyright (C) 1995-2020, Rene Brun and Fons Rademakers.               *
 * All rights reserved.                                                  *
 *                                                                       *
 * For the licensing terms see $ROOTSYS/LICENSE.                         *
 * For the list of contributors see $ROOTSYS/README/CREDITS.             *
 *************************************************************************/

#include <ROOT/RDaos.hxx>
#include <ROOT/RError.hxx>

#include <numeric>
#include <stdexcept>

ROOT::Experimental::Detail::RDaosPool::RDaosPool(std::string_view poolUuid, std::string_view serviceReplicas) {
   {
      static struct RDaosRAII {
         RDaosRAII() {
            daos_init();
         }
         ~RDaosRAII() { daos_fini(); }
      } RAII = {};
   }

   struct SvcRAII {
      d_rank_list_t *rankList;
      SvcRAII(std::string_view ranks) { rankList = daos_rank_list_parse(ranks.data(), "_"); }
      ~SvcRAII() { d_rank_list_free(rankList); }
   } Svc(serviceReplicas);
   daos_pool_info_t poolInfo{};

   uuid_parse(poolUuid.data(), fPoolUuid);
   if (int err = daos_pool_connect(fPoolUuid, nullptr, Svc.rankList, DAOS_PC_RW, &fPoolHandle, &poolInfo, nullptr))
      throw RException(R__FAIL("daos_pool_connect: error: " + std::string(d_errstr(err))));
}

ROOT::Experimental::Detail::RDaosPool::~RDaosPool() {
   daos_pool_disconnect(fPoolHandle, nullptr);
}


////////////////////////////////////////////////////////////////////////////////


std::string ROOT::Experimental::Detail::RDaosObject::ObjClassId::ToString() const
{
   char name[kOCNameMaxLength + 1] = {};
   daos_oclass_id2name(fCid, name);
   return std::string{name};
}


ROOT::Experimental::Detail::RDaosObject::FetchUpdateArgs::FetchUpdateArgs(FetchUpdateArgs&& fua)
  : fDkey(fua.fDkey), fAkey(fua.fAkey),
    fIods{fua.fIods[0]}, fSgls{fua.fSgls[0]}, fIovs(std::move(fua.fIovs)), fEv(fua.fEv)
{
   d_iov_set(&fDistributionKey, &fDkey, sizeof(fDkey));
   d_iov_set(&fIods[0].iod_name, &fAkey, sizeof(fAkey));
}

ROOT::Experimental::Detail::RDaosObject::FetchUpdateArgs::FetchUpdateArgs
(DistributionKey_t &d, AttributeKey_t &a, std::vector<d_iov_t> &v, daos_event_t *p)
  : fDkey(d), fAkey(a), fIovs(v), fEv(p)
{
   fNr = 1;
   d_iov_set(&fDistributionKey, &fDkey, sizeof(fDkey));

   d_iov_set(&fIods[0].iod_name, &fAkey, sizeof(fAkey));
   fIods[0].iod_nr = 1;
   fIods[0].iod_size = std::accumulate(v.begin(), v.end(), 0,
                                       [](daos_size_t _a, d_iov_t _b) { return _a + _b.iov_len; });
   fIods[0].iod_recxs = nullptr;
   fIods[0].iod_type = DAOS_IOD_SINGLE;

   fSgls[0].sg_nr_out = 0;
   fSgls[0].sg_nr = fIovs.size();
   fSgls[0].sg_iovs = fIovs.data();
}

ROOT::Experimental::Detail::RDaosObject::FetchUpdateArgs::FetchUpdateArgs
   (DistributionKey_t &d, std::vector<AttributeKey_t> &a, std::vector<d_iov_t> &v, unsigned nr, daos_event_t *p)
   : fDkey(d), fAkeys(a), fIovs_vec(v), fNr(nr), fEv(p)
{
   for (unsigned i = 0; i < nr; ++i){
      /* Sets distribution key, attribute key from local copies fDkey, fAkeys.
       * The distribution key is the same across attribute keys within the object. */
      d_iov_set(&fDistributionKey, &fDkey, sizeof(fDkey));

      daos_iod_t iod;
      iod.iod_nr = 1;   // One single IO vector provided for each value being read (i.e. each oid:dkey:akey)
      iod.iod_size = v[i].iov_len; // Each element of v corresponds to a single iov for its corresponding request
      iod.iod_recxs = nullptr;
      iod.iod_type = DAOS_IOD_SINGLE;
      fIods_vec.push_back(iod);
      d_iov_set(&fIods_vec[i].iod_name, &(fAkeys[i]), sizeof(fAkeys[i]));

      d_sg_list_t sgl;
      sgl.sg_nr_out = 0;
      sgl.sg_nr = 1;
      sgl.sg_iovs = (d_iov_t*) &fIovs_vec[i];
      fSgls_vec.push_back(sgl);
   }

}
/* Inserts an attribute key in a list of requested keys for I/O
 * \param d: DistributionKey_t must match the instance's value for distribution key
 *    unless the value is zero, in which case it initializes the attribute. */
int ROOT::Experimental::Detail::RDaosObject::FetchUpdateArgs::insert
   (DistributionKey_t &d, AttributeKey_t &a, d_iov_t &v) {

   if (fDkey == 0) {
      fDkey = d;
      d_iov_set(&fDistributionKey, &fDkey, sizeof(fDkey));
   }
   else if (fDkey != d) return -1;

   fAkeys.emplace_back(a);
   fIovs_vec.emplace_back(v);
   fNr = fAkeys.size();

   daos_iod_t iod;
   iod.iod_nr = 1;
   iod.iod_size = v.iov_len;
   iod.iod_recxs = nullptr;
   iod.iod_type = DAOS_IOD_SINGLE;
   fIods_vec.emplace_back(iod);
   d_iov_set(&fIods_vec.back().iod_name, &(fAkeys.back()), sizeof(fAkeys.back()));

   d_sg_list_t sgl;
   sgl.sg_nr_out = 0;
   sgl.sg_nr = 1;
   sgl.sg_iovs =  (d_iov_t*) &v;
   fSgls_vec.emplace_back(sgl);

   return 0;
}

/* Opens a new object in container for read-write operations,
 * Updates \a fObjectHandle with identifier to the object.
 * Updates \param oid with generated OID of the class ID if that is known.
 * */
ROOT::Experimental::Detail::RDaosObject::RDaosObject(RDaosContainer &container, daos_obj_id_t oid,
                                                     ObjClassId cid)
{
   if (!cid.IsUnknown())
      daos_obj_generate_id(&oid, DAOS_OF_DKEY_UINT64 | DAOS_OF_AKEY_UINT64 /*| DAOS_OF_ARRAY_BYTE*/, cid.fCid, 0);
   if (int err = daos_obj_open(container.fContainerHandle, oid, DAOS_OO_RW, &fObjectHandle, nullptr))
      throw RException(R__FAIL("daos_obj_open: error: " + std::string(d_errstr(err))));
}

ROOT::Experimental::Detail::RDaosObject::~RDaosObject()
{
   daos_obj_close(fObjectHandle, nullptr);
}

int ROOT::Experimental::Detail::RDaosObject::Fetch(FetchUpdateArgs &args)
{
   if (!args.fIods_vec.empty()){
      // NEW VERSION: adapted to consider multiple attributes
      for (daos_iod_t &iod : args.fIods_vec) {
         iod.iod_size = (daos_size_t)DAOS_REC_ANY;
      }
      return daos_obj_fetch(fObjectHandle, DAOS_TX_NONE,
                            DAOS_COND_DKEY_FETCH | DAOS_COND_AKEY_FETCH,
                            &args.fDistributionKey, args.fNr,
                            args.fIods_vec.data(), args.fSgls_vec.data(), nullptr, args.fEv);
   }
   else {
      // Legacy version (single dkey, akey per call to an object)
      args.fIods[0].iod_size = (daos_size_t)DAOS_REC_ANY;
      return daos_obj_fetch(fObjectHandle, DAOS_TX_NONE,
                            DAOS_COND_DKEY_FETCH | DAOS_COND_AKEY_FETCH,
                            &args.fDistributionKey, 1,
                            args.fIods, args.fSgls, nullptr, args.fEv);
   }
}

int ROOT::Experimental::Detail::RDaosObject::Update(FetchUpdateArgs &args)
{
   if (!args.fIods_vec.empty()) {
      // Generalized version for multiple attribute keys
      return daos_obj_update(fObjectHandle, DAOS_TX_NONE, 0,
                             &args.fDistributionKey, args.fNr,
                             args.fIods_vec.data(), args.fSgls_vec.data(), args.fEv);
   }
   else {
      // Legacy version
      return daos_obj_update(fObjectHandle, DAOS_TX_NONE, 0,
                             &args.fDistributionKey, 1,
                             args.fIods, args.fSgls, args.fEv);
   }
}


////////////////////////////////////////////////////////////////////////////////

//ROOT::Experimental::Detail::DaosEventQueue::DaosEventQueue(std::size_t size)
//   : fSize(size), fEvs(std::unique_ptr<daos_event_t[]>(new daos_event_t[size]))
//{
//   daos_eq_create(&fQueue);
//   for (std::size_t i = 0; i < fSize; ++i)
//      daos_event_init(&fEvs[i], fQueue, nullptr);
//}

ROOT::Experimental::Detail::DaosEventQueue::DaosEventQueue()
{
   fSize = fDefaultLength;
   daos_eq_create(&fQueue);
}

ROOT::Experimental::Detail::DaosEventQueue::~DaosEventQueue() {
   for (auto& [parent, events] : fEventMap) {
      daos_event_fini(parent.get());
   }
   daos_eq_destroy(fQueue, 0);
}

int ROOT::Experimental::Detail::DaosEventQueue::Poll() {
   auto evp = std::unique_ptr<daos_event_t*[]>(new daos_event_t*[fSize]);
   std::size_t n = fSize;
   while (n) {
      int c;
      if ((c = daos_eq_poll(fQueue, 0, DAOS_EQ_WAIT, n, evp.get())) < 0)
         break;
      n -= c;
   }
   return n;
}

int ROOT::Experimental::Detail::DaosEventQueue::PollEvent(std::unique_ptr<daos_event_t> ev) {

   bool flag = false;
   while (!flag) {
      if (int err = daos_event_test(ev.get(), 0, &flag) < 0) {
         throw RException(R__FAIL("daos_cont_open: error: " + std::string(d_errstr(err))));
      }
   }
   daos_event_fini(ev.get());  // NOTE: May not be necessary (check)
                               // since only reachable after test has completed the event
   if (fEventMap.count(ev) > 0)
      fEventMap.erase(ev);
   return 0;
}

int ROOT::Experimental::Detail::DaosEventQueue::PollUntilFree(unsigned int n_required) {
   auto evp = std::unique_ptr<daos_event_t*[]>(new daos_event_t*[fSize]);
   std::size_t n_outstanding = fSize;
   while ((fSize - n_outstanding) < n_required) {
      int c;
      if ((c = daos_eq_poll(fQueue, 0, DAOS_EQ_WAIT, n_outstanding, evp.get())) < 0)
         break;
      n_outstanding -= c;
   }
   return n_outstanding;
}


//int ROOT::Experimental::Detail::DaosEventQueue::Insert(unsigned int n_events, daos_event_t* evs) {
//
//   /* In this version the ROOT queue does not keep track of which jobs were completed,
//    * i.e. doesn't use the fEvs field. This is left for the DAOS-side. */
//   int rc;
//   // Query currently outstanding and wait for vacancies if necessary
//   int count_outstanding = daos_eq_query(fQueue, DAOS_EQR_ALL, fSize, NULL);
//   if ((fSize - count_outstanding) < n_events) {
//      // Poll until enough events reach completion
//      rc = PollWait(n_events - (fSize - count_outstanding));
//      if (rc < 0)
//         break;
//      count_outstanding -= rc;
//   }
//   // Insert each event
//   for (std::size_t i = 0; i < n_events; ++i) {
//      rc = daos_event_init(&evs[i], fQueue, nullptr);
//   }
//
//   return 0;
//}


//int ROOT::Experimental::Detail::DaosEventQueue::InsertEvents(unsigned int n_events, daos_event_t* evs) {
//
//   int count_outstanding = daos_eq_query(fQueue, DAOS_EQR_ALL, fSize, NULL);
//
//      if ((fSize - count_outstanding) < (n_events + 1)) {
//         int rc = PollUntilFree( (n_events + 1) - (fSize - count_outstanding));
//         if (rc < 0) return rc;
//      }
//
//   daos_event_t* parent = new daos_event_t;
//   int rc = daos_event_init(parent, fQueue, nullptr);
//
//   for (std::size_t i = 0; i < n_events; ++i) {
//      rc = daos_event_init(&evs[i], fQueue, parent);
//   }
//
//   return 0;
//}


////////////////////////////////////////////////////////////////////////////////


ROOT::Experimental::Detail::RDaosContainer::RDaosContainer(std::shared_ptr<RDaosPool> pool,
                                                           std::string_view containerUuid, bool create)
  : fPool(pool)
{
   daos_cont_info_t containerInfo{};

   uuid_parse(containerUuid.data(), fContainerUuid);
   if (create) {
      if (int err = daos_cont_create(fPool->fPoolHandle, fContainerUuid, nullptr, nullptr))
         throw RException(R__FAIL("daos_cont_create: error: " + std::string(d_errstr(err))));
   }
   if (int err = daos_cont_open(fPool->fPoolHandle, fContainerUuid, DAOS_COO_RW,
         &fContainerHandle, &containerInfo, nullptr))
      throw RException(R__FAIL("daos_cont_open: error: " + std::string(d_errstr(err))));
}

ROOT::Experimental::Detail::RDaosContainer::~RDaosContainer() {
   daos_cont_close(fContainerHandle, nullptr);
}

int ROOT::Experimental::Detail::RDaosContainer::ReadSingleAkey(void *buffer, std::size_t length, daos_obj_id_t oid,
                                                               DistributionKey_t dkey, AttributeKey_t akey,
                                                               ObjClassId_t cid)
{
   std::vector<d_iov_t> iovs(1);
   d_iov_set(&iovs[0], buffer, length);
   RDaosObject::FetchUpdateArgs args(dkey, akey, iovs);
   return RDaosObject(*this, oid, cid.fCid).Fetch(args);
}

int ROOT::Experimental::Detail::RDaosContainer::WriteSingleAkey(const void *buffer, std::size_t length, daos_obj_id_t oid,
                                                                DistributionKey_t dkey, AttributeKey_t akey,
                                                                ObjClassId_t cid)
{
   std::vector<d_iov_t> iovs(1);
   d_iov_set(&iovs[0], const_cast<void *>(buffer), length);
   RDaosObject::FetchUpdateArgs args(dkey, akey, iovs);
   return RDaosObject(*this, oid, cid.fCid).Update(args);
}

int ROOT::Experimental::Detail::RDaosContainer::ReadAttributeKeys(void *buffers[], std::size_t lengths[], daos_obj_id_t oid,
                                                          DistributionKey_t dkey, AttributeKey_t akeys[], unsigned nr,
                                                          ObjClassId_t cid)
{
   std::vector<d_iov_t> iovs_vec(nr);
   std::vector<AttributeKey_t> vec_akey(akeys, akeys + nr * sizeof(AttributeKey_t));
   for (unsigned i = 0; i < nr; ++i) {
      d_iov_set(&iovs_vec[i], buffers[i], lengths[i]);
   }
   RDaosObject::FetchUpdateArgs args(dkey, vec_akey, iovs_vec, nr);
   return RDaosObject(*this, oid, cid.fCid).Fetch(args);
}

int ROOT::Experimental::Detail::RDaosContainer::WriteAttributeKeys(const void *buffers[], std::size_t lengths[], daos_obj_id_t oid,
                                                                DistributionKey_t dkey, AttributeKey_t akeys[], unsigned nr,
                                                                ObjClassId_t cid)
{
   std::vector<d_iov_t> iovs_vec(nr);
   std::vector<AttributeKey_t> vec_akey(akeys, akeys + nr * sizeof(AttributeKey_t));

   for (unsigned i = 0; i < nr; ++i) {
      d_iov_set(&iovs_vec[i], const_cast<void *>(buffers[i]), lengths[i]);
   }
   RDaosObject::FetchUpdateArgs args(dkey, vec_akey, iovs_vec, nr);
   return RDaosObject(*this, oid, cid.fCid).Update(args);
}
