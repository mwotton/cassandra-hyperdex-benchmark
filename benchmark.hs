{-# LANGUAGE OverloadedStrings #-}
import Criterion.Main
import Criterion.Config
import Control.Concurrent(threadDelay)
import qualified Database.HyperDex as H
import qualified Database.Cassandra.Basic as C
import System.Environment(getEnv)
import Control.Monad(forM_,void)
import Control.Applicative
import Control.DeepSeq

import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy.Char8 as BSL
import qualified Data.Text as Text
import System.Cmd
import Data.Monoid

instance NFData H.Attribute where
  rnf (H.Attribute a b c) = a `seq` b `seq` c `seq` ()



main :: IO ()
main = do
  reps <- read  <$> getEnv "REPS"
  system "cqlsh < ./go.cql"
  ws <- BS.lines <$> BS.readFile "/usr/share/dict/words"
  wsl <- BSL.lines <$> BSL.readFile "/usr/share/dict/words"

  let first = BS.unlines $!! take 1000 ws
      lastname = BS.unlines $!! take 10000 ws
      firstl = BSL.unlines $!! take 1000 wsl
      lastnamel = BSL.unlines $!! take 10000 wsl
      testrun = take reps $!! ws
      testrunl = take reps $!! wsl
  print (length testrun)

  pool <- C.createCassandraPool C.defServers 3 300 5 "testkeyspace"

  client <- H.connect H.defaultConnectInfo
  H.addSpace client $ Text.unlines
    [ "space phonebook"
    , "key username"
    , "attributes first, last"
    , "subspace first, last"
    , "create 8 partitions"
    ]
  defaultMain [
    bench "hyperdex" $ do
       forM_ testrun $ \w -> do
          H.put client "phonebook" w $!!
            [H.mkAttribute "first" first
            ,H.mkAttribute "last"  lastname
            ],

    bench "cassandra" $
      forM_ testrunl $ \w ->
        C.runCas pool $
          C.insert  "phonebook" w C.QUORUM
            [ C.col "first" firstl
            , C.col "last" lastnamel
            ]]
  void $ system "echo 'use testkeyspace; drop table phonebook;' | cqlsh"
  void $ H.removeSpace client "phonebook"
  -- defaultMain [ bench "hyperdex" $
  -- defaultMain [ bench "
