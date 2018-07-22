{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Shared.Schema where

import RIO

import qualified RIO.Time as Time

import Data.Proxy(Proxy(..))
import Test.QuickCheck.Arbitrary
import Test.QuickCheck.Instances()
import Test.QuickCheck as QC
import qualified Generics.SOP as SOP
import qualified Generics.SOP.Arbitrary as SOP
import qualified Data.Aeson as Aeson
import qualified Data.IxSet.Typed as IxSet
import Acid.World
import Codec.Serialise
import Data.SafeCopy


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
  userCreated :: !(Maybe Time.UTCTime),
  userDisabled :: !Bool
} deriving (Eq, Show, Generic, Ord)

instance Serialise User

type UserIxs = '[Int, Maybe Time.UTCTime, Bool]
type UserIxSet = IxSet.IxSet UserIxs User
instance IxSet.Indexable UserIxs User where
  indices = IxSet.ixList
              (IxSet.ixFun $ (:[]) . userId )
              (IxSet.ixFun $ (:[]) . userCreated )
              (IxSet.ixFun $ (:[]) . userDisabled )

instance Aeson.ToJSON User
instance Aeson.FromJSON User
instance SOP.Generic User
instance Arbitrary User where arbitrary = SOP.garbitrary
instance NFData User

instance Segment "Users" where
  type SegmentS "Users" = UserIxSet
  defaultState _ = IxSet.empty

insertUser :: (ValidAcidWorldState i ss, HasValidSegment i ss  "Users") => User -> AWUpdate i ss User
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

insertUserWithBoolReturn :: (ValidAcidWorldState i ss, HasValidSegment i ss  "Users") => User -> AWUpdate i ss Bool
insertUserWithBoolReturn a = do
  u <- insertUser a
  return $ userDisabled u

instance Eventable "insertUserWithBoolReturn" where
  type EventArgs "insertUserWithBoolReturn" = '[User]
  type EventResult "insertUserWithBoolReturn" = Bool
  type EventSegments "insertUserWithBoolReturn" = '["Users"]
  runEvent _ = toRunEvent insertUserWithBoolReturn

fetchUsers :: (ValidAcidWorldState i ss, HasValidSegment i ss  "Users") => AWQuery i ss [User]
fetchUsers = fmap IxSet.toList $ askSegment (Proxy :: Proxy "Users")

fetchUsersStats :: (ValidAcidWorldState i ss, HasValidSegment i ss  "Users") => AWQuery i ss Int
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


insertAddress :: (ValidAcidWorldState i ss, HasSegment ss  "Addresses") => Address -> AWUpdate i ss Address
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



fetchAddresses :: (ValidAcidWorldState i ss, HasSegment ss  "Addresses") => AWQuery i ss [Address]
fetchAddresses = fmap IxSet.toList $ askSegment (Proxy :: Proxy "Addresses")

fetchAddressesStats :: (ValidAcidWorldState i ss, HasSegment ss  "Addresses") => AWQuery i ss Int
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


insertPhonenumber :: (ValidAcidWorldState i ss, HasSegment ss  "Phonenumbers") => Phonenumber -> AWUpdate i ss Phonenumber
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



fetchPhonenumbers :: (ValidAcidWorldState i ss, HasSegment ss  "Phonenumbers") => AWQuery i ss [Phonenumber]
fetchPhonenumbers = fmap IxSet.toList $ askSegment (Proxy :: Proxy "Phonenumbers")

fetchPhonenumbersStats :: (ValidAcidWorldState i ss, HasSegment ss  "Phonenumbers") => AWQuery i ss Int
fetchPhonenumbersStats = fmap IxSet.size $ askSegment (Proxy :: Proxy "Phonenumbers")



generatePhonenumbers :: Int -> Gen [Phonenumber]
generatePhonenumbers i = do
  us <- sequence $ replicate i (arbitrary)
  pure $ map (\(u, uid) -> u{phonenumberId = uid}) $ zip us [1..]


deriveSafeCopy 0 'base ''User
deriveSafeCopy 0 'base ''Address
deriveSafeCopy 0 'base ''Phonenumber