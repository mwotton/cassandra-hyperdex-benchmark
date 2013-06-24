{-# LANGUAGE OverloadedStrings #-}
import Criterion.Main
import qualified Database.HyperDex as H
import qualified Database.Cassandra.Basic as C
import System.Environment(getEnv)
import Control.Monad(forM_,forM,void,when,join,forever)
import Control.Applicative
import Control.DeepSeq
import Data.Time.Clock
import qualified Control.Concurrent.Thread.Group as TG
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy.Char8 as BSL
import qualified Data.Text as Text
import qualified Data.Text.IO as Text
import Data.Either(lefts)

import System.Cmd
import Control.Proxy.Aeson
import Control.Proxy.Trans.Identity
import Control.Proxy.Class
import Control.Proxy
import Control.Monad.Trans.Class
import System.IO
import qualified Database.SQLite3 as SQL
instance NFData H.Attribute where
  rnf (H.Attribute a b c) = a `seq` b `seq` c `seq` ()

main :: IO ()
main = do
  reps <- read  <$> getEnv "REPS"
  -- threaded <- (=="yes")  <$> getEnv "THREADED"
  system "cqlsh < ./go.cql"
  system "rm dummysql"

  ws <- BS.lines <$> BS.readFile "/usr/share/dict/words"
  wsl <- BSL.lines <$> BSL.readFile "/usr/share/dict/words"
  wst <- Text.lines <$> Text.readFile "/usr/share/dict/words"

  let first = BS.unlines $!! take 1000 ws
      firstl = BSL.unlines $!! take 1000 wsl
      firstt = Text.unlines $!! take 1000 wst

      lastname = BS.unlines $!! take 10000 ws
      lastnamel = BSL.unlines $!! take 10000 wsl
      lastnamet = Text.unlines $!! take 10000 wst

      testrun = take reps $!! ws
      testrunl = take reps $!! wsl
      testrunt = take reps $!! wst

  return $ rnf first
  return $ rnf lastname
--  return $ rnf testrun
  return $ rnf firstt
  return $ rnf lastnamet
  --return $ rnf testrunt
  return $ rnf firstl
  return $ rnf lastnamel
--  return $ rnf testrunl



  pool <- C.createCassandraPool C.defServers 3 300 5 "testkeyspace"

  client <- H.connect H.defaultConnectInfo
  H.addSpace client $ Text.unlines
    [ "space phonebook"
    , "key username"
    , "attributes first, last"
    -- , "subspace first, last"
    , "create 8 partitions"
    ]
  db <- SQL.open "dummysql"
  SQL.execPrint db "PRAGMA journal_mode=MEMORY; PRAGMA synchronous = OFF"

  SQL.exec db "create table phonebook (username txt, firstname text, lastname text);"
  stmt <- SQL.prepare db "insert into phonebook (username, firstname,lastname) values (?, ?,?);"


  --file <- openFile "/dev/null" WriteMode

  let -- finish :: ((BS.ByteString, BSL.ByteString, Text.Text) -> IO a) -> ([a] -> IO ()) -> IO ()
      finish work ender = mapM work (zip3 testrun testrunl testrunt) >>= ender

  defaultMain [bench "sqlite" $ do
                  SQL.execPrint db "begin transaction;"
                  finish ( \(_,_,x) -> do
                           SQL.bind stmt [SQL.SQLText x, SQL.SQLText firstt, SQL.SQLText lastnamet]
                           SQL.step stmt
                           SQL.reset stmt)
                    ( \_ -> SQL.execPrint db "end transaction;"  ),
               bench "file" $ do
                 file <- openFile "./dummyfile" WriteMode
                 finish (\(x,_,_) ->
                          BS.hPutStrLn file $ BS.unlines [x, first, lastname])
                   (\_ -> hClose file),
               bench "cassandra" $ finish (\(_,x,_) -> return $
                                              C.insert  "phonebook" x C.ANY
                                                [ C.col "first" firstl
                                                , C.col "last" lastnamel
                                                ])
                                          (\insertions ->  void $ C.runCas pool $ sequence insertions),
               bench "hyperdex" $ finish (\(x,_,_) ->
                                           H.put client "phonebook" "foo" $!!
                                             [H.mkAttribute "first" first
                                             ,H.mkAttribute "last"  lastname])
                                         (\actions -> do
                                             failures <- lefts <$> sequence actions
                                             when (failures /= []) $
                                               error ("failure in hyperdex: " ++ show failures)
                                             return ())
              ]


  void $ system "echo 'use testkeyspace; drop table phonebook;' | cqlsh"
  void $ H.removeSpace client "phonebook"
