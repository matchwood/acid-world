module Acid.Core.Serialise.JSON.Partial where

import RIO
import qualified  RIO.Text as T
import qualified  RIO.ByteString.Lazy as BL
import qualified  RIO.ByteString as BS

--import Control.Arrow (left)
import Generics.SOP


import qualified Data.Aeson as Aeson
import Data.Aeson(FromJSON(..), Value(..))
import Data.Aeson.Internal (ifromJSON, IResult(..), formatError)
import qualified Data.Attoparsec.ByteString as Atto
import qualified Data.Attoparsec.ByteString.Lazy as Atto.L
import Acid.Core.Utils

{- Partial decoding for json -}
eitherPartialDecode' :: (FromJSON a) => BS.ByteString -> JSONResult a
eitherPartialDecode' = eitherPartialDecodeWith Aeson.json' ifromJSON

eitherPartialDecodeWith :: forall a. Atto.Parser Value -> (Value -> IResult a) -> BS.ByteString
                 -> JSONResult a
eitherPartialDecodeWith p tra s = handleRes (Atto.parse p s)
  where
    handleRes :: Atto.IResult BS.ByteString Value -> JSONResult a
    handleRes (Atto.Done i v) =
        case tra v of
          ISuccess a      -> JSONResultDone (i, a)
          IError path msg -> JSONResultFail (T.pack $ formatError path msg)
    handleRes (Atto.Partial np) = JSONResultPartial (handleRes . np)
    handleRes (Atto.Fail _ _ msg) = JSONResultFail (T.pack msg)

data JSONResult a =
    JSONResultFail Text
  | JSONResultPartial (BS.ByteString -> JSONResult a)
  | JSONResultDone (BS.ByteString, a)





{- Decoding with remainder - needed because of how we serialise event names -}
eitherDecodeLeftover :: (FromJSON a) => BL.ByteString -> Either Text (BL.ByteString, a)
eitherDecodeLeftover = eitherDecodeLeftoverWithParser  ifromJSON

eitherDecodeLeftoverWithParser :: (Value -> IResult a) -> BL.ByteString -> Either Text (BL.ByteString, a)
eitherDecodeLeftoverWithParser p = eitherDecodeLeftoverWith Aeson.json' p

eitherDecodeLeftoverWith :: Atto.L.Parser Value -> (Value -> IResult a) -> BL.ByteString
                 -> Either Text (BL.ByteString, a)
eitherDecodeLeftoverWith p tra s =
    case Atto.L.parse p s of
      Atto.L.Done bs v     -> case tra v of
                          ISuccess a      -> pure (bs, a)
                          IError path msg -> Left (T.pack $ formatError path msg)
      Atto.L.Fail _ _ msg -> Left (T.pack msg)

consumeMatchAndParse :: forall a b. (FromJSON a, FromJSON b, Eq a, Show a) => Proxy a -> a -> BL.ByteString -> Either Text b
consumeMatchAndParse _ aMatch bs = do
  (bs', (a :: a)) <- eitherDecodeLeftover bs
  if a == aMatch
    then fmap snd $ eitherDecodeLeftover bs'
    else Left $ "Expected " <> showT aMatch <> " when consuming prefix, but got " <> showT a
