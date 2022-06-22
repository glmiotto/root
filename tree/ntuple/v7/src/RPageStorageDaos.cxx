/// \file RPageStorageDaos.cxx
/// \ingroup NTuple ROOT7
/// \author Javier Lopez-Gomez <j.lopez@cern.ch>
/// \date 2020-11-03
/// \warning This is part of the ROOT 7 prototype! It will change without notice. It might trigger earthquakes. Feedback
/// is welcome!

/*************************************************************************
 * Copyright (C) 1995-2021, Rene Brun and Fons Rademakers.               *
 * All rights reserved.                                                  *
 *                                                                       *
 * For the licensing terms see $ROOTSYS/LICENSE.                         *
 * For the list of contributors see $ROOTSYS/README/CREDITS.             *
 *************************************************************************/

#include <ROOT/RCluster.hxx>
#include <ROOT/RClusterPool.hxx>
#include <ROOT/RField.hxx>
#include <ROOT/RLogger.hxx>
#include <ROOT/RNTupleDescriptor.hxx>
#include <ROOT/RNTupleModel.hxx>
#include <ROOT/RNTupleSerialize.hxx>
#include <ROOT/RNTupleUtil.hxx>
#include <ROOT/RNTupleZip.hxx>
#include <ROOT/RPage.hxx>
#include <ROOT/RPageAllocator.hxx>
#include <ROOT/RPagePool.hxx>
#include <ROOT/RDaos.hxx>
#include <ROOT/RPageStorageDaos.hxx>

#include <RVersion.h>
#include <TError.h>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <utility>
#include <regex>

namespace {
struct RDaosURI {
   /// \brief Label of the DAOS pool
   std::string fPoolLabel;
   /// \brief Label of the container for this RNTuple
   std::string fContainerLabel;
};

/**
  \brief Parse a DAOS RNTuple URI of the form 'daos://pool_id/container_id'.
*/
RDaosURI ParseDaosURI(std::string_view uri)
{
   std::regex re("daos://([^/]+)/(.+)");
   std::cmatch m;
   if (!std::regex_match(uri.data(), m, re))
      throw ROOT::Experimental::RException(R__FAIL("Invalid DAOS pool URI."));
   return {m[1], m[2]};
}


/// \brief Some random distribution/attribute key.
static constexpr std::uint64_t kDistributionKey = 0x5a3c69f0cafe4a11;      // Legacy
static constexpr std::uint64_t kDistributionKeyMetadata = 0x5a3c69f0cafe4912;

static constexpr std::uint64_t kAttributeKey = 0x4243544b5344422d;         // Legacy

static constexpr std::uint64_t kAttributeKeyAnchor = 0x4243544b5344422d;
static constexpr std::uint64_t kAttributeKeyHeader = 0x4243544b5344421e;
static constexpr std::uint64_t kAttributeKeyFooter = 0x4243544b5344420f;

//static constexpr daos_obj_id_t kOidAnchor{std::uint64_t(-1), 0};           // Legacy
//static constexpr daos_obj_id_t kOidHeader{std::uint64_t(-2), 0};           // Legacy
//static constexpr daos_obj_id_t kOidFooter{std::uint64_t(-3), 0};           // Legacy

static constexpr daos_obj_id_t kOidMetadata{std::uint64_t(-11), 0};
static constexpr daos_obj_id_t kOidPageList{std::uint64_t(-12), 0};

static constexpr daos_oclass_id_t kCidMetadata = OC_SX;
} // namespace


////////////////////////////////////////////////////////////////////////////////

RDaosKV 
GetDaosKVPage(daos_mapping_t strategy, ROOT::Experimental::Detail::RDaosSealedPageLocator &loc, unsigned clusterId, long unsigned count) {
   switch(strategy) {
      case O_CLUSTER_D_COLUMN: 
         return RDaosKV{daos_obj_id_t{clusterId, 0}, 
         reinterpret_cast<DistributionKey_t>(loc.fColumnId), reinterpret_cast<AttributeKey_t>(count)};
      case O_UNIQUE: 
         return RDaosKV{daos_obj_id_t{loc.fObjectId, 0}, kDistributionKey, kAttributeKey};
   }
}

RDaosKV 
GetDaosKVMetadata(daos_kv_t type, daos_mapping_t strategy)
{
   if (type == DAOS_HEADER) {
      switch (strategy) {
      case O_CLUSTER_D_COLUMN: return {kOidMetadata, kDistributionKeyMetadata, kAttributeKeyHeader};
      case O_UNIQUE: return RDaosKV{kOidHeader, kDistributionKey, kAttributeKey};
      }
   } else if (type == DAOS_ANCHOR) {
      switch (strategy) {
      case O_CLUSTER_D_COLUMN: return {kOidMetadata, kDistributionKeyMetadata, kAttributeKeyAnchor};
      case O_UNIQUE: return RDaosKV{kOidAnchor, kDistributionKey, kAttributeKey};
      }
   } else if (type == DAOS_FOOTER) {
      switch (strategy) {
      case O_CLUSTER_D_COLUMN: return {kOidMetadata, kDistributionKeyMetadata, kAttributeKeyFooter};
      case O_UNIQUE: return RDaosKV{kOidFooter, kDistributionKey, kAttributeKey};
      }
   } else return {daos_obj_id_t{}, DistributionKey_t{}, AttributeKey_t{}};
}


std::uint32_t
ROOT::Experimental::Detail::RDaosNTupleAnchor::Serialize(void *buffer) const
{
   using RNTupleSerializer = ROOT::Experimental::Internal::RNTupleSerializer;
   if (buffer != nullptr) {
      auto bytes = reinterpret_cast<unsigned char *>(buffer);
      bytes += RNTupleSerializer::SerializeUInt32(fVersion, bytes);
      bytes += RNTupleSerializer::SerializeUInt32(fNBytesHeader, bytes);
      bytes += RNTupleSerializer::SerializeUInt32(fLenHeader, bytes);
      bytes += RNTupleSerializer::SerializeUInt32(fNBytesFooter, bytes);
      bytes += RNTupleSerializer::SerializeUInt32(fLenFooter, bytes);
      bytes += RNTupleSerializer::SerializeString(fObjClass, bytes);
   }
   return RNTupleSerializer::SerializeString(fObjClass, nullptr) + 20;
}

ROOT::Experimental::RResult<std::uint32_t>
ROOT::Experimental::Detail::RDaosNTupleAnchor::Deserialize(const void *buffer, std::uint32_t bufSize)
{
   if (bufSize < 20)
      return R__FAIL("DAOS anchor too short");

   using RNTupleSerializer = ROOT::Experimental::Internal::RNTupleSerializer;
   auto bytes = reinterpret_cast<const unsigned char *>(buffer);
   bytes += RNTupleSerializer::DeserializeUInt32(bytes, fVersion);
   bytes += RNTupleSerializer::DeserializeUInt32(bytes, fNBytesHeader);
   bytes += RNTupleSerializer::DeserializeUInt32(bytes, fLenHeader);
   bytes += RNTupleSerializer::DeserializeUInt32(bytes, fNBytesFooter);
   bytes += RNTupleSerializer::DeserializeUInt32(bytes, fLenFooter);
   auto result = RNTupleSerializer::DeserializeString(bytes, bufSize - 20, fObjClass);
   if (!result)
      return R__FORWARD_ERROR(result);
   return result.Unwrap() + 20;
}

std::uint32_t
ROOT::Experimental::Detail::RDaosNTupleAnchor::GetSize()
{
   return RDaosNTupleAnchor().Serialize(nullptr)
      + ROOT::Experimental::Detail::RDaosObject::ObjClassId::kOCNameMaxLength;
}


////////////////////////////////////////////////////////////////////////////////


ROOT::Experimental::Detail::RPageSinkDaos::RPageSinkDaos(std::string_view ntupleName, std::string_view uri,
   const RNTupleWriteOptions &options)
   : RPageSink(ntupleName, options)
   , fPageAllocator(std::make_unique<RPageAllocatorHeap>())
   , fURI(uri)
{
   R__LOG_WARNING(NTupleLog()) << "The DAOS backend is experimental and still under development. " <<
      "Do not store real data with this version of RNTuple!";
   fCompressor = std::make_unique<RNTupleCompressor>();
   EnableDefaultMetrics("RPageSinkDaos");
}


ROOT::Experimental::Detail::RPageSinkDaos::~RPageSinkDaos() = default;

void ROOT::Experimental::Detail::RPageSinkDaos::CreateImpl(const RNTupleModel & /* model */,
                                                           unsigned char *serializedHeader, std::uint32_t length)
{
   auto opts = dynamic_cast<RNTupleWriteOptionsDaos *>(fOptions.get());
   fNTupleAnchor.fObjClass = opts ? opts->GetObjectClass() : RNTupleWriteOptionsDaos().GetObjectClass();
   auto oclass = RDaosObject::ObjClassId(fNTupleAnchor.fObjClass);
   if (oclass.IsUnknown())
      throw ROOT::Experimental::RException(R__FAIL("Unknown object class " + fNTupleAnchor.fObjClass));

   auto args = ParseDaosURI(fURI);
   auto pool = std::make_shared<RDaosPool>(args.fPoolLabel);
   fDaosContainer = std::make_unique<RDaosContainer>(pool, args.fContainerLabel, /*create =*/true);
   fDaosContainer->SetDefaultObjectClass(oclass);

   auto zipBuffer = std::make_unique<unsigned char[]>(length);
   auto szZipHeader = fCompressor->Zip(serializedHeader, length, GetWriteOptions().GetCompression(),
                                       RNTupleCompressor::MakeMemCopyWriter(zipBuffer.get()));
   WriteNTupleHeader(zipBuffer.get(), szZipHeader, length);
}


ROOT::Experimental::RNTupleLocator
ROOT::Experimental::Detail::RPageSinkDaos::CommitPageImpl(ColumnHandle_t columnHandle, const RPage &page)
{
   auto element = columnHandle.fColumn->GetElement();
   RPageStorage::RSealedPage sealedPage;
   {
      RNTupleAtomicTimer timer(fCounters->fTimeWallZip, fCounters->fTimeCpuZip);
      sealedPage = SealPage(page, *element, GetWriteOptions().GetCompression());
   }

   fCounters->fSzZip.Add(page.GetNBytes());
   return CommitSealedPageImpl(columnHandle.fId, sealedPage);
}


ROOT::Experimental::RNTupleLocator
ROOT::Experimental::Detail::RPageSinkDaos::CommitSealedPageImpl(
   DescriptorId_t columnId, const RPageStorage::RSealedPage &sealedPage)
{
   auto offsetData = fOid.fetch_add(1);
   DescriptorId_t clusterId = fDescriptorBuilder.GetDescriptor().GetNClusters();

   {
      RNTupleAtomicTimer timer(fCounters->fTimeWallWrite, fCounters->fTimeCpuWrite);
      RDaosKey store_key = GetDaosPageKey<kOClusterDColumn>(clusterId, columnId, offsetData);
      fDaosContainer->WriteSingleAkey(sealedPage.fBuffer, sealedPage.fSize,
                                      store_key.oid, store_key.d_key, store_key.a_key);
   }

   RNTupleLocator result;
   result.fPosition = offsetData;
   result.fBytesOnStorage = sealedPage.fSize;
   fCounters->fNPageCommitted.Inc();
   fCounters->fSzWritePayload.Add(sealedPage.fSize);
   fNBytesCurrentCluster += sealedPage.fSize;
   return result;
}


std::uint64_t
ROOT::Experimental::Detail::RPageSinkDaos::CommitClusterImpl(ROOT::Experimental::NTupleSize_t /* nEntries */)
{
   return std::exchange(fNBytesCurrentCluster, 0);
}

ROOT::Experimental::RNTupleLocator
ROOT::Experimental::Detail::RPageSinkDaos::CommitClusterGroupImpl(unsigned char *serializedPageList,
                                                                  std::uint32_t length)
{
   auto bufPageListZip = std::make_unique<unsigned char[]>(length);
   auto szPageListZip = fCompressor->Zip(serializedPageList, length, GetWriteOptions().GetCompression(),
                                         RNTupleCompressor::MakeMemCopyWriter(bufPageListZip.get()));

   auto offsetData = fOid.fetch_add(1);
   fDaosContainer->WriteSingleAkey(bufPageListZip.get(), szPageListZip, kOidPageList, kDistributionKeyMetadata,
                                   offsetData, kCidMetadata);
   RNTupleLocator result;
   result.fPosition = offsetData;
   result.fBytesOnStorage = szPageListZip;
   fCounters->fSzWritePayload.Add(szPageListZip);
   return result;
}

void ROOT::Experimental::Detail::RPageSinkDaos::CommitDatasetImpl(unsigned char *serializedFooter, std::uint32_t length)
{
   auto bufFooterZip = std::make_unique<unsigned char[]>(length);
   auto szFooterZip = fCompressor->Zip(serializedFooter, length, GetWriteOptions().GetCompression(),
                                       RNTupleCompressor::MakeMemCopyWriter(bufFooterZip.get()));
   WriteNTupleFooter(bufFooterZip.get(), szFooterZip, length);
   WriteNTupleAnchor();
}

void ROOT::Experimental::Detail::RPageSinkDaos::WriteNTupleHeader(
		const void *data, size_t nbytes, size_t lenHeader)
{
   fDaosContainer->WriteSingleAkey(data, nbytes, kOidMetadata, kDistributionKeyMetadata,
                                   kAttributeKeyHeader, kCidMetadata);
   fNTupleAnchor.fLenHeader = lenHeader;
   fNTupleAnchor.fNBytesHeader = nbytes;
}

void ROOT::Experimental::Detail::RPageSinkDaos::WriteNTupleFooter(
		const void *data, size_t nbytes, size_t lenFooter)
{
   fDaosContainer->WriteSingleAkey(data, nbytes, kOidMetadata, kDistributionKeyMetadata,
                                   kAttributeKeyFooter, kCidMetadata);
   fNTupleAnchor.fLenFooter = lenFooter;
   fNTupleAnchor.fNBytesFooter = nbytes;
}

void ROOT::Experimental::Detail::RPageSinkDaos::WriteNTupleAnchor() {
   const auto ntplSize = RDaosNTupleAnchor::GetSize();
   auto buffer = std::make_unique<unsigned char[]>(ntplSize);
   fNTupleAnchor.Serialize(buffer.get());
   fDaosContainer->WriteSingleAkey(buffer.get(), ntplSize, kOidMetadata, kDistributionKeyMetadata,
                                   kAttributeKeyAnchor, kCidMetadata);
}

ROOT::Experimental::Detail::RPage
ROOT::Experimental::Detail::RPageSinkDaos::ReservePage(ColumnHandle_t columnHandle, std::size_t nElements)
{
   if (nElements == 0)
      throw RException(R__FAIL("invalid call: request empty page"));
   auto elementSize = columnHandle.fColumn->GetElement()->GetSize();
   return fPageAllocator->NewPage(columnHandle.fId, elementSize, nElements);
}

void ROOT::Experimental::Detail::RPageSinkDaos::ReleasePage(RPage &page)
{
   fPageAllocator->DeletePage(page);
}


////////////////////////////////////////////////////////////////////////////////


ROOT::Experimental::Detail::RPage ROOT::Experimental::Detail::RPageAllocatorDaos::NewPage(
   ColumnId_t columnId, void *mem, std::size_t elementSize, std::size_t nElements)
{
   RPage newPage(columnId, mem, elementSize, nElements);
   newPage.GrowUnchecked(nElements);
   return newPage;
}

void ROOT::Experimental::Detail::RPageAllocatorDaos::DeletePage(const RPage& page)
{
   if (page.IsNull())
      return;
   delete[] reinterpret_cast<unsigned char *>(page.GetBuffer());
}


////////////////////////////////////////////////////////////////////////////////


ROOT::Experimental::Detail::RPageSourceDaos::RPageSourceDaos(std::string_view ntupleName, std::string_view uri,
   const RNTupleReadOptions &options)
   : RPageSource(ntupleName, options)
   , fPageAllocator(std::make_unique<RPageAllocatorDaos>())
   , fPagePool(std::make_shared<RPagePool>())
   , fURI(uri)
   , fClusterPool(std::make_unique<RClusterPool>(*this))
{
   fDecompressor = std::make_unique<RNTupleDecompressor>();
   EnableDefaultMetrics("RPageSourceDaos");

   auto args = ParseDaosURI(uri);
   auto pool = std::make_shared<RDaosPool>(args.fPoolLabel);
   fDaosContainer = std::make_unique<RDaosContainer>(pool, args.fContainerLabel);
}


ROOT::Experimental::Detail::RPageSourceDaos::~RPageSourceDaos() = default;


ROOT::Experimental::RNTupleDescriptor ROOT::Experimental::Detail::RPageSourceDaos::AttachImpl()
{
   RNTupleDescriptorBuilder descBuilder;
   RDaosNTupleAnchor ntpl;
   const auto ntplSize = RDaosNTupleAnchor::GetSize();
   auto buffer = std::make_unique<unsigned char[]>(ntplSize);
   fDaosContainer->ReadSingleAkey(buffer.get(), ntplSize, kOidMetadata, kDistributionKeyMetadata,
                                  kAttributeKeyAnchor, kCidMetadata);
   ntpl.Deserialize(buffer.get(), ntplSize).Unwrap();

   auto oclass = RDaosObject::ObjClassId(ntpl.fObjClass);
   if (oclass.IsUnknown())
      throw ROOT::Experimental::RException(R__FAIL("Unknown object class " + ntpl.fObjClass));
   fDaosContainer->SetDefaultObjectClass(oclass);

   descBuilder.SetOnDiskHeaderSize(ntpl.fNBytesHeader);
   buffer = std::make_unique<unsigned char[]>(ntpl.fLenHeader);
   auto zipBuffer = std::make_unique<unsigned char[]>(ntpl.fNBytesHeader);
   fDaosContainer->ReadSingleAkey(zipBuffer.get(), ntpl.fNBytesHeader, kOidMetadata, kDistributionKeyMetadata,
                                  kAttributeKeyHeader, kCidMetadata);
   fDecompressor->Unzip(zipBuffer.get(), ntpl.fNBytesHeader, ntpl.fLenHeader, buffer.get());
   Internal::RNTupleSerializer::DeserializeHeaderV1(buffer.get(), ntpl.fLenHeader, descBuilder);

   descBuilder.AddToOnDiskFooterSize(ntpl.fNBytesFooter);
   buffer = std::make_unique<unsigned char[]>(ntpl.fLenFooter);
   zipBuffer = std::make_unique<unsigned char[]>(ntpl.fNBytesFooter);
   fDaosContainer->ReadSingleAkey(zipBuffer.get(), ntpl.fNBytesFooter, kOidMetadata, kDistributionKeyMetadata,
                                  kAttributeKeyFooter, kCidMetadata);
   fDecompressor->Unzip(zipBuffer.get(), ntpl.fNBytesFooter, ntpl.fLenFooter, buffer.get());
   Internal::RNTupleSerializer::DeserializeFooterV1(buffer.get(), ntpl.fLenFooter, descBuilder);

   auto ntplDesc = descBuilder.MoveDescriptor();

   for (const auto &cgDesc : ntplDesc.GetClusterGroupIterable()) {
      buffer = std::make_unique<unsigned char[]>(cgDesc.GetPageListLength());
      zipBuffer = std::make_unique<unsigned char[]>(cgDesc.GetPageListLocator().fBytesOnStorage);
      fDaosContainer->ReadSingleAkey(zipBuffer.get(), cgDesc.GetPageListLocator().fBytesOnStorage, kOidPageList,
                                     kDistributionKeyMetadata, cgDesc.GetPageListLocator().fPosition, kCidMetadata);
      fDecompressor->Unzip(zipBuffer.get(), cgDesc.GetPageListLocator().fBytesOnStorage, cgDesc.GetPageListLength(),
                           buffer.get());

      auto clusters = RClusterGroupDescriptorBuilder::GetClusterSummaries(ntplDesc, cgDesc.GetId());
      Internal::RNTupleSerializer::DeserializePageListV1(buffer.get(), cgDesc.GetPageListLength(), clusters);
      for (std::size_t i = 0; i < clusters.size(); ++i) {
         ntplDesc.AddClusterDetails(clusters[i].MoveDescriptor().Unwrap());
      }
   }

   return ntplDesc;
}


std::string ROOT::Experimental::Detail::RPageSourceDaos::GetObjectClass() const
{
   return fDaosContainer->GetDefaultObjectClass().ToString();
}


void ROOT::Experimental::Detail::RPageSourceDaos::LoadSealedPage(
   DescriptorId_t columnId, const RClusterIndex &clusterIndex, RSealedPage &sealedPage)
{
   const auto clusterId = clusterIndex.GetClusterId();

   RClusterDescriptor::RPageRange::RPageInfo pageInfo;
   {
      auto descriptorGuard = GetSharedDescriptorGuard();
      const auto &clusterDescriptor = descriptorGuard->GetClusterDescriptor(clusterId);
      pageInfo = clusterDescriptor.GetPageRange(columnId).Find(clusterIndex.GetIndex());
   }

   const auto bytesOnStorage = pageInfo.fLocator.fBytesOnStorage;
   sealedPage.fSize = bytesOnStorage;
   sealedPage.fNElements = pageInfo.fNElements;
   if (sealedPage.fBuffer) {
      RDaosKey key = GetDaosPageKey<kOClusterDColumn>(clusterId, columnId, pageInfo.fLocator.fPosition);
      fDaosContainer->ReadSingleAkey(const_cast<void *>(sealedPage.fBuffer), bytesOnStorage,
                                     key.oid, key.d_key, key.a_key);
   }
}

ROOT::Experimental::Detail::RPage
ROOT::Experimental::Detail::RPageSourceDaos::PopulatePageFromCluster(ColumnHandle_t columnHandle,
                                                                     const RClusterInfo &clusterInfo,
                                                                     ClusterSize_t::ValueType idxInCluster)
{
   const auto columnId = columnHandle.fId;
   const auto clusterId = clusterInfo.fClusterId;
   const auto &pageInfo = clusterInfo.fPageInfo;

   const auto element = columnHandle.fColumn->GetElement();
   const auto elementSize = element->GetSize();
   const auto bytesOnStorage = pageInfo.fLocator.fBytesOnStorage;

   const void *sealedPageBuffer = nullptr; // points either to directReadBuffer or to a read-only page in the cluster
   std::unique_ptr<unsigned char []> directReadBuffer; // only used if cluster pool is turned off

   if (fOptions.GetClusterCache() == RNTupleReadOptions::EClusterCache::kOff) {
      directReadBuffer = std::make_unique<unsigned char[]>(bytesOnStorage);
      RDaosKey key = GetDaosPageKey<kOClusterDColumn>(clusterId, columnId, pageInfo.fLocator.fPosition);
      fDaosContainer->ReadSingleAkey(directReadBuffer.get(), bytesOnStorage,
                                     key.oid, key.d_key, key.a_key);
      fCounters->fNPageLoaded.Inc();
      fCounters->fNRead.Inc();
      fCounters->fSzReadPayload.Add(bytesOnStorage);
      sealedPageBuffer = directReadBuffer.get();
   } else {
      if (!fCurrentCluster || (fCurrentCluster->GetId() != clusterId) || !fCurrentCluster->ContainsColumn(columnId))
         fCurrentCluster = fClusterPool->GetCluster(clusterId, fActiveColumns);
      R__ASSERT(fCurrentCluster->ContainsColumn(columnId));

      auto cachedPage = fPagePool->GetPage(columnId, RClusterIndex(clusterId, idxInCluster));
      if (!cachedPage.IsNull())
         return cachedPage;

      ROnDiskPage::Key key(columnId, pageInfo.fPageNo);
      auto onDiskPage = fCurrentCluster->GetOnDiskPage(key);
      R__ASSERT(onDiskPage && (bytesOnStorage == onDiskPage->GetSize()));
      sealedPageBuffer = onDiskPage->GetAddress();
   }

   std::unique_ptr<unsigned char []> pageBuffer;
   {
      RNTupleAtomicTimer timer(fCounters->fTimeWallUnzip, fCounters->fTimeCpuUnzip);
      pageBuffer = UnsealPage({sealedPageBuffer, bytesOnStorage, pageInfo.fNElements}, *element);
      fCounters->fSzUnzip.Add(elementSize * pageInfo.fNElements);
   }

   auto newPage = fPageAllocator->NewPage(columnId, pageBuffer.release(), elementSize, pageInfo.fNElements);
   newPage.SetWindow(clusterInfo.fColumnOffset + pageInfo.fFirstInPage,
                     RPage::RClusterInfo(clusterId, clusterInfo.fColumnOffset));
   fPagePool->RegisterPage(newPage,
      RPageDeleter([](const RPage &page, void * /*userData*/)
      {
         RPageAllocatorDaos::DeletePage(page);
      }, nullptr));
   fCounters->fNPagePopulated.Inc();
   return newPage;
}


ROOT::Experimental::Detail::RPage ROOT::Experimental::Detail::RPageSourceDaos::PopulatePage(
   ColumnHandle_t columnHandle, NTupleSize_t globalIndex)
{
   const auto columnId = columnHandle.fId;
   auto cachedPage = fPagePool->GetPage(columnId, globalIndex);
   if (!cachedPage.IsNull())
      return cachedPage;

   std::uint64_t idxInCluster;
   RClusterInfo clusterInfo;
   {
      auto descriptorGuard = GetSharedDescriptorGuard();
      clusterInfo.fClusterId = descriptorGuard->FindClusterId(columnId, globalIndex);
      R__ASSERT(clusterInfo.fClusterId != kInvalidDescriptorId);

      const auto &clusterDescriptor = descriptorGuard->GetClusterDescriptor(clusterInfo.fClusterId);
      clusterInfo.fColumnOffset = clusterDescriptor.GetColumnRange(columnId).fFirstElementIndex;
      R__ASSERT(clusterInfo.fColumnOffset <= globalIndex);
      idxInCluster = globalIndex - clusterInfo.fColumnOffset;
      clusterInfo.fPageInfo = clusterDescriptor.GetPageRange(columnId).Find(idxInCluster);
   }
   return PopulatePageFromCluster(columnHandle, clusterInfo, idxInCluster);
}


ROOT::Experimental::Detail::RPage ROOT::Experimental::Detail::RPageSourceDaos::PopulatePage(
   ColumnHandle_t columnHandle, const RClusterIndex &clusterIndex)
{
   const auto clusterId = clusterIndex.GetClusterId();
   const auto idxInCluster = clusterIndex.GetIndex();
   const auto columnId = columnHandle.fId;
   auto cachedPage = fPagePool->GetPage(columnId, clusterIndex);
   if (!cachedPage.IsNull())
      return cachedPage;

   R__ASSERT(clusterId != kInvalidDescriptorId);
   RClusterInfo clusterInfo;
   {
      auto descriptorGuard = GetSharedDescriptorGuard();
      const auto &clusterDescriptor = descriptorGuard->GetClusterDescriptor(clusterId);
      clusterInfo.fClusterId = clusterId;
      clusterInfo.fColumnOffset = clusterDescriptor.GetColumnRange(columnId).fFirstElementIndex;
      clusterInfo.fPageInfo = clusterDescriptor.GetPageRange(columnId).Find(idxInCluster);
   }

   return PopulatePageFromCluster(columnHandle, clusterInfo, idxInCluster);
}

void ROOT::Experimental::Detail::RPageSourceDaos::ReleasePage(RPage &page)
{
   fPagePool->ReturnPage(page);
}

std::unique_ptr<ROOT::Experimental::Detail::RPageSource> ROOT::Experimental::Detail::RPageSourceDaos::Clone() const
{
   auto clone = new RPageSourceDaos(fNTupleName, fURI, fOptions);
   return std::unique_ptr<RPageSourceDaos>(clone);
}

std::vector<std::unique_ptr<ROOT::Experimental::Detail::RCluster>>
ROOT::Experimental::Detail::RPageSourceDaos::LoadClusters(std::span<RCluster::RKey> clusterKeys)
{
   std::vector<std::unique_ptr<ROOT::Experimental::Detail::RCluster>> result;
   for (const auto &clusterKey : clusterKeys) {
      auto clusterId = clusterKey.fClusterId;
      fCounters->fNClusterLoaded.Inc();

      std::vector<RDaosSealedPageLocator> onDiskPages;
      std::size_t szPayload = 0;
      {
         auto descriptorGuard = GetSharedDescriptorGuard();
         const auto &clusterDesc = descriptorGuard->GetClusterDescriptor(clusterId);

         // Collect the page necessary page meta-data and sum up the total size of the compressed and packed pages
         for (auto columnId : clusterKey.fColumnSet) {
            const auto &pageRange = clusterDesc.GetPageRange(columnId);
            NTupleSize_t pageNo = 0;
            for (const auto &pageInfo : pageRange.fPageInfos) {
               const auto &pageLocator = pageInfo.fLocator;
               onDiskPages.emplace_back(RDaosSealedPageLocator(columnId, pageNo, pageLocator.fPosition,
                                                               pageLocator.fBytesOnStorage, szPayload));
               szPayload += pageLocator.fBytesOnStorage;
               ++pageNo;
            }
         }
      }

      // Prepare the input map for the RDaosContainer::ReadV() call
      std::unordered_map<std::pair<daos_obj_id_t, DistributionKey_t>, RDaosContainer::RWOperation> readRequests;
      std::vector<d_iov_t> iovs(onDiskPages.size());
      auto buffer = new unsigned char[szPayload];

      for (unsigned i = 0; i < onDiskPages.size(); ++i) {
         auto &s = onDiskPages[i];
         d_iov_set(&iovs[i], buffer + s.fBufPos, s.fSize);

         RDaosKey store_key = GetDaosPageKey<kOClusterDColumn>(clusterId, s.fColumnId, s.fObjectId);
         auto dict_key = std::make_pair(store_key.oid, store_key.d_key);
         if (readRequests.count(dict_key) == 0) {
            readRequests[dict_key].insert(store_key.oid, store_key.d_key, store_key.a_key, iovs[i]);
         }
         else {
            readRequests[dict_key].insert(store_key.a_key, iovs[i]);
         }
      }
      fCounters->fSzReadPayload.Add(szPayload);

      // Register the on disk pages in a page map
      auto pageMap = std::make_unique<ROnDiskPageMapHeap>(std::unique_ptr<unsigned char []>(buffer));
      for (const auto &s : onDiskPages) {
         ROnDiskPage::Key key(s.fColumnId, s.fPageNo);
         pageMap->Register(key, ROnDiskPage(buffer + s.fBufPos, s.fSize));
      }
      fCounters->fNPageLoaded.Add(onDiskPages.size());

      {
         RNTupleAtomicTimer timer(fCounters->fTimeWallRead, fCounters->fTimeCpuRead);
         if (int err = fDaosContainer->ReadV(readRequests))
            throw ROOT::Experimental::RException(R__FAIL("ReadV: error" + std::string(d_errstr(err))));
      }
      fCounters->fNReadV.Inc();
      fCounters->fNRead.Add(readRequests.size());

      auto cluster = std::make_unique<RCluster>(clusterId);
      cluster->Adopt(std::move(pageMap));
      for (auto colId : clusterKey.fColumnSet)
         cluster->SetColumnAvailable(colId);

      result.emplace_back(std::move(cluster));
   }
   return result;
}


void ROOT::Experimental::Detail::RPageSourceDaos::UnzipClusterImpl(RCluster *cluster)
{
   RNTupleAtomicTimer timer(fCounters->fTimeWallUnzip, fCounters->fTimeCpuUnzip);
   fTaskScheduler->Reset();

   const auto clusterId = cluster->GetId();
   auto descriptorGuard = GetSharedDescriptorGuard();
   const auto &clusterDescriptor = descriptorGuard->GetClusterDescriptor(clusterId);

   std::vector<std::unique_ptr<RColumnElementBase>> allElements;

   const auto &columnsInCluster = cluster->GetAvailColumns();
   for (const auto columnId : columnsInCluster) {
      const auto &columnDesc = descriptorGuard->GetColumnDescriptor(columnId);

      allElements.emplace_back(RColumnElementBase::Generate(columnDesc.GetModel().GetType()));

      const auto &pageRange = clusterDescriptor.GetPageRange(columnId);
      std::uint64_t pageNo = 0;
      std::uint64_t firstInPage = 0;
      for (const auto &pi : pageRange.fPageInfos) {
         ROnDiskPage::Key key(columnId, pageNo);
         auto onDiskPage = cluster->GetOnDiskPage(key);
         R__ASSERT(onDiskPage && (onDiskPage->GetSize() == pi.fLocator.fBytesOnStorage));

         auto taskFunc =
            [this, columnId, clusterId, firstInPage, onDiskPage,
             element = allElements.back().get(),
             nElements = pi.fNElements,
             indexOffset = clusterDescriptor.GetColumnRange(columnId).fFirstElementIndex
            ] () {
               auto pageBuffer = UnsealPage({onDiskPage->GetAddress(), onDiskPage->GetSize(), nElements}, *element);
               fCounters->fSzUnzip.Add(element->GetSize() * nElements);

               auto newPage = fPageAllocator->NewPage(columnId, pageBuffer.release(), element->GetSize(), nElements);
               newPage.SetWindow(indexOffset + firstInPage, RPage::RClusterInfo(clusterId, indexOffset));
               fPagePool->PreloadPage(newPage,
                  RPageDeleter([](const RPage &page, void * /*userData*/)
                  {
                     RPageAllocatorDaos::DeletePage(page);
                  }, nullptr));
            };

         fTaskScheduler->AddTask(taskFunc);

         firstInPage += pi.fNElements;
         pageNo++;
      } // for all pages in column
   } // for all columns in cluster

   fCounters->fNPagePopulated.Add(cluster->GetNOnDiskPages());

   fTaskScheduler->Wait();
}
