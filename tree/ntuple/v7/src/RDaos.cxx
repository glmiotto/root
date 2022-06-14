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

ROOT::Experimental::Detail::RDaosPool::RDaosPool(std::string_view poolLabel)
{
   {
      static struct RDaosRAII {
         RDaosRAII() { daos_init(); }
         ~RDaosRAII() { daos_fini(); }
      } RAII = {};
   }

   fPoolLabel = poolLabel;

   daos_pool_info_t poolInfo{};
   if (int err = daos_pool_connect(fPoolLabel.data(), nullptr, DAOS_PC_RW, &fPoolHandle, &poolInfo, nullptr)) {
      throw RException(R__FAIL("daos_pool_connect: error: " + std::string(d_errstr(err))));
   }

   fEventQueue.Initialize();
}

ROOT::Experimental::Detail::RDaosPool::~RDaosPool()
{
   daos_pool_disconnect(fPoolHandle, nullptr);
}

////////////////////////////////////////////////////////////////////////////////


std::string ROOT::Experimental::Detail::RDaosObject::ObjClassId::ToString() const
{
   char name[kOCNameMaxLength + 1] = {};
   daos_oclass_id2name(fCid, name);
   return std::string{name};
}

ROOT::Experimental::Detail::RDaosObject::FetchUpdateArgs::FetchUpdateArgs(FetchUpdateArgs &&fua)
   : fDkey(fua.fDkey), fAkeys(std::move(fua.fAkeys)), fIods(std::move(fua.fIods)), fSgls(std::move(fua.fSgls)), fIovs(std::move(fua.fIovs)),
     fEvent(std::move(fua.fEvent)), fIsAsync(fua.fIsAsync)
{
   d_iov_set(&fDistributionKey, &fDkey, sizeof(fDkey));
   for (unsigned i = 0; i < fAkeys.size(); ++i) {
      d_iov_set(&fIods[i].iod_name, &(fAkeys[i]), sizeof(fAkeys[i]));
   }
}

ROOT::Experimental::Detail::RDaosObject::FetchUpdateArgs::FetchUpdateArgs(DistributionKey_t &d, std::vector<AttributeKey_t> &&as, std::vector<d_iov_t> &&vs, bool is_async)
   : fDkey(d), fAkeys(std::move(as)), fIovs(std::move(vs)), fIsAsync(is_async)
{
   fSgls.reserve(fAkeys.size());
   fIods.reserve(fAkeys.size());
   d_iov_set(&fDistributionKey, &fDkey, sizeof(fDkey));

   for (unsigned i = 0; i < fAkeys.size(); ++i){
      daos_iod_t iod;
      iod.iod_nr = 1;
      iod.iod_size = fIovs[i].iov_len;
      iod.iod_recxs = nullptr;
      iod.iod_type = DAOS_IOD_SINGLE;
      fIods.push_back(iod);
      d_iov_set(&fIods[i].iod_name, &(fAkeys[i]), sizeof(fAkeys[i]));

      d_sg_list_t sgl;
      sgl.sg_nr_out = 0;
      sgl.sg_nr = 1;
      sgl.sg_iovs = (d_iov_t*) &fIovs[i];
      fSgls.push_back(sgl);
   }
}

daos_event_t *ROOT::Experimental::Detail::RDaosObject::FetchUpdateArgs::GetEventPointer()
{
   if (fIsAsync)
      return &fEvent;
   return nullptr;
}

ROOT::Experimental::Detail::RDaosObject::RDaosObject(RDaosContainer &container, daos_obj_id_t oid,
                                                     ObjClassId cid)
{
   if (!cid.IsUnknown())
      daos_obj_generate_oid(container.fContainerHandle, &oid,
                            static_cast<daos_otype_t>(DAOS_OT_DKEY_UINT64 | DAOS_OT_AKEY_UINT64), cid.fCid,
                            DAOS_OCH_RDD_DEF, 0);

   if (int err = daos_obj_open(container.fContainerHandle, oid, DAOS_OO_RW, &fObjectHandle, nullptr))
      throw RException(R__FAIL("daos_obj_open: error: " + std::string(d_errstr(err))));
}

ROOT::Experimental::Detail::RDaosObject::~RDaosObject()
{
   daos_obj_close(fObjectHandle, nullptr);
}

int ROOT::Experimental::Detail::RDaosObject::Fetch(FetchUpdateArgs &args)
{
   for (daos_iod_t &iod : args.fIods) {
      iod.iod_size = (daos_size_t)DAOS_REC_ANY;
   }

   return daos_obj_fetch(fObjectHandle, DAOS_TX_NONE, DAOS_COND_DKEY_FETCH | DAOS_COND_AKEY_FETCH,
                         &args.fDistributionKey, args.fAkeys.size(), args.fIods.data(), args.fSgls.data(), nullptr,
                         args.GetEventPointer());
}

int ROOT::Experimental::Detail::RDaosObject::Update(FetchUpdateArgs &args)
{
   return daos_obj_update(fObjectHandle, DAOS_TX_NONE, 0, &args.fDistributionKey, args.fAkeys.size(), args.fIods.data(),
                          args.fSgls.data(), args.GetEventPointer());
}

////////////////////////////////////////////////////////////////////////////////

ROOT::Experimental::Detail::DaosEventQueue::~DaosEventQueue()
{
   Destroy();
}

void ROOT::Experimental::Detail::DaosEventQueue::Initialize()
{
   if (int ret = daos_eq_create(&fQueue) < 0)
      throw RException(R__FAIL("daos_eq_create: error: " + std::string(d_errstr(ret))));
}

void ROOT::Experimental::Detail::DaosEventQueue::Destroy()
{
   if (int err = daos_eq_destroy(fQueue, 0) < 0) {
      throw RException(R__FAIL("daos_eq_destroy: error: " + std::string(d_errstr(err))));
   }
}

int ROOT::Experimental::Detail::DaosEventQueue::InitializeEvent(daos_event_t *ev_ptr, daos_event_t *parent_ptr)
{
   if (int err = daos_event_init(ev_ptr, fQueue, parent_ptr) < 0) {
      throw RException(R__FAIL("daos_event_init: error: " + std::string(d_errstr(err))));
   }
   return 0;
}

int ROOT::Experimental::Detail::DaosEventQueue::FinalizeEvent(daos_event_t *ev_ptr)
{
   if (int err = daos_event_fini(ev_ptr) < 0) {
      throw RException(R__FAIL("daos_event_fini: error: " + std::string(d_errstr(err))));
   }
   return 0;
}

int ROOT::Experimental::Detail::DaosEventQueue::PollEvent(daos_event_t *ev_ptr)
{
   bool flag = false;
   while (!flag) {
      if (int err = daos_event_test(ev_ptr, 0, &flag) < 0) {
         throw RException(R__FAIL("daos_event_test: error: " + std::string(d_errstr(err))));
      }
   }
   int ret = FinalizeEvent(ev_ptr);
   return ret;
}

int ROOT::Experimental::Detail::DaosEventQueue::LaunchParentBarrier(daos_event_t *ev_ptr)
{
   if (int err = daos_event_parent_barrier(ev_ptr) < 0) {
      throw RException(R__FAIL("daos_event_parent_barrier: error: " + std::string(d_errstr(err))));
   }
   return 0;
}

////////////////////////////////////////////////////////////////////////////////

ROOT::Experimental::Detail::RDaosContainer::RDaosContainer(std::shared_ptr<RDaosPool> pool,
                                                           std::string_view containerLabel, bool create)
   : fPool(pool)
{
   daos_cont_info_t containerInfo{};

   fContainerLabel = containerLabel;

   if (create) {
      if (int err =
             daos_cont_create_with_label(fPool->fPoolHandle, fContainerLabel.data(), nullptr, nullptr, nullptr)) {
         if (err != -DER_EXIST)
            throw RException(R__FAIL("daos_cont_create_with_label: error: " + std::string(d_errstr(err))));
      }
   }
   if (int err = daos_cont_open(fPool->fPoolHandle, fContainerLabel.data(), DAOS_COO_RW, &fContainerHandle,
                                &containerInfo, nullptr))
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
   std::vector<AttributeKey_t> v_akey{akey};
   RDaosObject::FetchUpdateArgs args(dkey, std::move(v_akey), std::move(iovs), false);
   return RDaosObject(*this, oid, cid.fCid).Fetch(args);
}

int ROOT::Experimental::Detail::RDaosContainer::WriteSingleAkey(const void *buffer, std::size_t length, daos_obj_id_t oid,
                                                                DistributionKey_t dkey, AttributeKey_t akey,
                                                                ObjClassId_t cid)
{
   std::vector<d_iov_t> iovs(1);
   d_iov_set(&iovs[0], const_cast<void *>(buffer), length);
   std::vector<AttributeKey_t> v_akey{akey};
   RDaosObject::FetchUpdateArgs args(dkey, std::move(v_akey), std::move(iovs), false);
   return RDaosObject(*this, oid, cid.fCid).Update(args);
}

int ROOT::Experimental::Detail::RDaosContainer::VectorReadWrite(std::vector<RWOperation> &vec, ObjClassId_t cid, 
   std::_Mem_fn<int(RDaosObject::*)(RDaosObject::FetchUpdateArgs&)> fn) {
      int ret;
      {
         using request_t = std::tuple<std::unique_ptr<RDaosObject>, RDaosObject::FetchUpdateArgs>;

         std::vector<request_t> requests{};
         requests.reserve(vec.size());

         daos_event_t parent_event{};
         fPool->fEventQueue.InitializeEvent(&parent_event);

         for (size_t i = 0; i < vec.size(); ++i) {
            requests.push_back(std::make_tuple(
               std::make_unique<RDaosObject>(*this, vec[i].fOid, cid.fCid),
               RDaosObject::FetchUpdateArgs{vec[i].fDistributionKey, std::move(vec[i].fAttributeKeys), std::move(vec[i].fIovs), true}));

            // Initialize child event with parent
            fPool->fEventQueue.InitializeEvent(&(std::get<1>(requests.back()).fEvent), &parent_event);

            // Launch operation request
            if ((ret = fn(std::get<0>(requests.back()).get(), std::get<1>(requests.back()))) < 0)
               return ret;
         }

         if ((ret = fPool->fEventQueue.LaunchParentBarrier(&parent_event)) < 0)
            return ret;

         // Poll until completion of all children launched before the barrier
         ret = fPool->fEventQueue.PollEvent(&parent_event);
      }
      return ret;
   }
