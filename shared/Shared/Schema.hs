{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Shared.Schema where

import RIO
import qualified RIO.HashMap as HM
import qualified RIO.Text as T

import qualified RIO.Time as Time

import Data.Proxy(Proxy(..))
import Test.QuickCheck.Arbitrary
import Test.QuickCheck.Instances()
import Test.QuickCheck as QC
import qualified Generics.SOP as SOP
import qualified Generics.SOP.Arbitrary as SOP
import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Types as Aeson
import qualified Data.IxSet.Typed as IxSet
import Acid.World
import Codec.Serialise
import Data.SafeCopy
import Acid.Core.State.CacheState

import qualified Database.PostgreSQL.Simple.ToRow as PSQL
import qualified Database.PostgreSQL.Simple.ToField as PSQL
import qualified Database.PostgreSQL.Simple.FromField as PSQL

instance (Serialise b, IxSet.Indexable a b) => Serialise (IxSet.IxSet a b) where
  encode = encode . IxSet.toList
  decode = fmap IxSet.fromList $ decode

instance (Aeson.ToJSON b) => Aeson.ToJSON (IxSet.IxSet a b) where
  toJSON = Aeson.toJSON . IxSet.toList

instance (Aeson.FromJSON b, IxSet.Indexable a b) => Aeson.FromJSON (IxSet.IxSet a b) where
  parseJSON v = fmap IxSet.fromList $ Aeson.parseJSON v


data User = User  {
  userId :: !Int,
  userFirstName :: !Text,
  userLastName :: !Text,
  userComments :: [Text],
  userOtherInformation :: Text,
  userCreated :: !(Maybe Time.UTCTime),
  userDisabled :: !Bool
} deriving (Eq, Show, Generic, Ord)

instance PSQL.ToField [Text] where
  toField = PSQL.toField . Aeson.toJSON

instance PSQL.FromField [Text] where
  fromField f bs = do
   v <- PSQL.fromField f bs
   case Aeson.parseEither Aeson.parseJSON v of
    Left err -> PSQL.conversionError $ AWExceptionEventDeserialisationError (T.pack err)
    Right ts -> pure ts

instance PSQL.ToRow User
instance (IxSet.Indexable a User) => AcidSerialisePostgres (IxSet.IxSet a User) where
  toPostgresRows a = map PostgresRow (IxSet.toList a)
  createTable _ = mconcat ["CREATE TABLE ", fromString (tableName (Proxy :: Proxy "Users")) ," (userId integer NOT NULL, userFirstName Text NOT NULL, userLastName Text NOT NULL, userComments json NOT NULL, userOtherInformation Text NOT NULL, userCreated timestamptz, userDisabled bool);"]
  fromPostgresConduitT ts = fmap IxSet.fromList $ sequence $ map fromFields ts


instance FromFields User where
  fromFields fs = do
    userId <- fromIdx fs 0
    userFirstName <- fromIdx fs 1
    userLastName <- fromIdx fs 2
    userComments <- fromIdx fs 3
    userOtherInformation <- fromIdx fs 4
    userCreated <- fromIdx fs 5
    userDisabled <- fromIdx fs 6
    pure $ User{..}



instance Serialise User

type UserIxs = '[Int, Maybe Time.UTCTime, Bool]
type UserIxSet = IxSet.IxSet UserIxs User
instance IxSet.Indexable UserIxs User where
  indices = IxSet.ixList
              (IxSet.ixFun $ (:[]) . userId )
              (IxSet.ixFun $ (:[]) . userCreated )
              (IxSet.ixFun $ (:[]) . userDisabled )

type UserCSIxSet = IxSet.IxSet UserIxs (CValIxs UserIxs User)


instance Aeson.ToJSON User
instance Aeson.FromJSON User
instance SOP.Generic User
instance Arbitrary User where arbitrary = SOP.garbitrary
instance NFData User

instance Segment "Users" where
  type SegmentS "Users" = UserIxSet
  defaultState _ = IxSet.empty

instance Segment "UsersHM" where
  type SegmentS "UsersHM" = HM.HashMap Int (CVal User)
  defaultState _ = HM.empty

instance SegmentC "UsersHM"

instance Segment "UsersCS" where
  type SegmentS "UsersCS" = UserCSIxSet
  defaultState _ = IxSet.empty

instance SegmentC "UsersCS"

instance IxsetPrimaryKeyClass (IxSet.IxSet UserIxs (CValIxs UserIxs User)) where
  type IxsetPrimaryKey UserCSIxSet = Int


insertUser :: (ValidAcidWorldState i ss, HasSegmentAndInvar ss  "Users") => User -> AWUpdate i ss User
insertUser a = do
  ls <- getSegment (Proxy :: Proxy "Users")
  let newLs = IxSet.insert a ls
  putSegment (Proxy :: Proxy "Users") newLs
  return a

instance Eventable "insertUser" where
  type EventArgs "insertUser" = '[User]
  type EventResult "insertUser" = User
  type EventSegments "insertUser" = '["Users"]
  runEvent _ = toRunEvent insertUser

insertUserWithBoolReturn :: (ValidAcidWorldState i ss, HasSegmentAndInvar ss  "Users") => User -> AWUpdate i ss Bool
insertUserWithBoolReturn a = do
  u <- insertUser a
  return $ userDisabled u

instance Eventable "insertUserWithBoolReturn" where
  type EventArgs "insertUserWithBoolReturn" = '[User]
  type EventResult "insertUserWithBoolReturn" = Bool
  type EventSegments "insertUserWithBoolReturn" = '["Users"]
  runEvent _ = toRunEvent insertUserWithBoolReturn

fetchUsers :: (ValidAcidWorldState i ss, HasSegmentAndInvar ss  "Users") => AWQuery i ss [User]
fetchUsers = fmap IxSet.toList $ askSegment (Proxy :: Proxy "Users")

fetchUsersStats :: (ValidAcidWorldState i ss, HasSegmentAndInvar ss  "Users") => AWQuery i ss Int
fetchUsersStats = fmap IxSet.size $ askSegment (Proxy :: Proxy "Users")


generateUserIO :: IO User
generateUserIO = QC.generate arbitrary

generateUsers :: Int -> Gen [User]
generateUsers i = do
  us <- sequence $ replicate i (arbitrary)
  pure $ map (\(u, uid) -> u{userId = uid}) $ zip us [1..]




data Address = Address  {
  addressId :: !Int,
  addressFirst :: !Text,
  addressCountry :: !Text
} deriving (Eq, Show, Generic, Ord)

instance PSQL.ToRow Address
instance (IxSet.Indexable a Address) =>  AcidSerialisePostgres (IxSet.IxSet a Address) where
  toPostgresRows a = map PostgresRow (IxSet.toList a)
  createTable _ = mconcat ["CREATE TABLE ", fromString (tableName (Proxy :: Proxy "Addresses")) ," (addressId integer NOT NULL, addressFirst Text NOT NULL, addressCountry Text NOT NULL);"]
  fromPostgresConduitT ts = fmap IxSet.fromList $ sequence $ map fromFields ts


instance FromFields Address where
  fromFields fs = do
    addressId <- fromIdx fs 0
    addressFirst <- fromIdx fs 1
    addressCountry <- fromIdx fs 2
    pure $ Address{..}


instance Serialise Address

type AddressIxs = '[Int, Text]
type AddressIxSet = IxSet.IxSet AddressIxs Address
instance IxSet.Indexable AddressIxs Address where
  indices = IxSet.ixList
              (IxSet.ixFun $ (:[]) . addressId )
              (IxSet.ixFun $ (:[]) . addressCountry )

instance Aeson.ToJSON Address
instance Aeson.FromJSON Address
instance SOP.Generic Address
instance Arbitrary Address where arbitrary = SOP.garbitrary
instance NFData Address

instance Segment "Addresses" where
  type SegmentS "Addresses" = AddressIxSet
  defaultState _ = IxSet.empty


instance Segment "AddressesHM" where
  type SegmentS "AddressesHM" = HM.HashMap Int (CVal Address)
  defaultState _ = HM.empty

instance SegmentC "AddressesHM"


insertAddress :: (ValidAcidWorldState i ss, HasSegmentAndInvar ss  "Addresses") => Address -> AWUpdate i ss Address
insertAddress a = do
  ls <- getSegment (Proxy :: Proxy "Addresses")
  let newLs = IxSet.insert a ls
  putSegment (Proxy :: Proxy "Addresses") newLs
  return a

instance Eventable "insertAddress" where
  type EventArgs "insertAddress" = '[Address]
  type EventResult "insertAddress" = Address
  type EventSegments "insertAddress" = '["Addresses"]
  runEvent _ = toRunEvent insertAddress



fetchAddresses :: (ValidAcidWorldState i ss, HasSegmentAndInvar ss  "Addresses") => AWQuery i ss [Address]
fetchAddresses = fmap IxSet.toList $ askSegment (Proxy :: Proxy "Addresses")

fetchAddressesStats :: (ValidAcidWorldState i ss, HasSegmentAndInvar ss  "Addresses") => AWQuery i ss Int
fetchAddressesStats = fmap IxSet.size $ askSegment (Proxy :: Proxy "Addresses")

generateAddresses :: Int -> Gen [Address]
generateAddresses i = do
  us <- sequence $ replicate i (arbitrary)
  pure $ map (\(u, uid) -> u{addressId = uid}) $ zip us [1..]


data Phonenumber = Phonenumber  {
  phonenumberId :: !Int,
  phonenumberNumber :: !Text,
  phonenumberCallingCode :: !Int,
  phonenumberIsCell :: !Bool
} deriving (Eq, Show, Generic, Ord)

instance PSQL.ToRow Phonenumber
instance (IxSet.Indexable a Phonenumber) => AcidSerialisePostgres (IxSet.IxSet a Phonenumber) where
  toPostgresRows a = map PostgresRow (IxSet.toList a)
  createTable _ = mconcat ["CREATE TABLE ", fromString (tableName (Proxy :: Proxy "Phonenumbers")) ," (phonenumberId integer NOT NULL, phonenumberNumber Text NOT NULL, phonenumberCallingCode integer NOT NULL,  phonenumberIsCell bool);"]
  fromPostgresConduitT ts = fmap IxSet.fromList $ sequence $ map fromFields ts

instance FromFields Phonenumber where
  fromFields fs = do
    phonenumberId <- fromIdx fs 0
    phonenumberNumber <- fromIdx fs 1
    phonenumberCallingCode <- fromIdx fs 2
    phonenumberIsCell <- fromIdx fs 3
    pure $ Phonenumber{..}

instance Serialise Phonenumber

type PhonenumberIxs = '[Int, Int, Bool]
type PhonenumberIxSet = IxSet.IxSet PhonenumberIxs Phonenumber
instance IxSet.Indexable PhonenumberIxs Phonenumber where
  indices = IxSet.ixList
              (IxSet.ixFun $ (:[]) . phonenumberId )
              (IxSet.ixFun $ (:[]) . phonenumberCallingCode )
              (IxSet.ixFun $ (:[]) . phonenumberIsCell )

instance Aeson.ToJSON Phonenumber
instance Aeson.FromJSON Phonenumber
instance SOP.Generic Phonenumber
instance Arbitrary Phonenumber where arbitrary = SOP.garbitrary
instance NFData Phonenumber

instance Segment "Phonenumbers" where
  type SegmentS "Phonenumbers" = PhonenumberIxSet
  defaultState _ = IxSet.empty

instance Segment "PhonenumbersHM" where
  type SegmentS "PhonenumbersHM" = HM.HashMap Int (CVal Phonenumber)
  defaultState _ = HM.empty

instance SegmentC "PhonenumbersHM"

insertPhonenumber :: (ValidAcidWorldState i ss, HasSegmentAndInvar ss  "Phonenumbers") => Phonenumber -> AWUpdate i ss Phonenumber
insertPhonenumber a = do
  ls <- getSegment (Proxy :: Proxy "Phonenumbers")
  let newLs = IxSet.insert a ls
  putSegment (Proxy :: Proxy "Phonenumbers") newLs
  return a

instance Eventable "insertPhonenumber" where
  type EventArgs "insertPhonenumber" = '[Phonenumber]
  type EventResult "insertPhonenumber" = Phonenumber
  type EventSegments "insertPhonenumber" = '["Phonenumbers"]
  runEvent _ = toRunEvent insertPhonenumber



fetchPhonenumbers :: (ValidAcidWorldState i ss, HasSegmentAndInvar ss  "Phonenumbers") => AWQuery i ss [Phonenumber]
fetchPhonenumbers = fmap IxSet.toList $ askSegment (Proxy :: Proxy "Phonenumbers")

fetchPhonenumbersStats :: (ValidAcidWorldState i ss, HasSegmentAndInvar ss  "Phonenumbers") => AWQuery i ss Int
fetchPhonenumbersStats = fmap IxSet.size $ askSegment (Proxy :: Proxy "Phonenumbers")



generatePhonenumbers :: Int -> Gen [Phonenumber]
generatePhonenumbers i = do
  us <- sequence $ replicate i (arbitrary)
  pure $ map (\(u, uid) -> u{phonenumberId = uid}) $ zip us [1..]


deriveSafeCopy 0 'base ''User
deriveSafeCopy 0 'base ''Address
deriveSafeCopy 0 'base ''Phonenumber