//
//  MultithreadingTests.swift
//  VergeORM
//
//  Created by muukii on 2020/03/17.
//  Copyright © 2020 muukii. All rights reserved.
//

import Foundation

import XCTest

import VergeRx
import VergeStore
import VergeORM

class MultithreadingTests: XCTestCase {
  
  let store = Storage(RootState())
      
  func testUpdateFromThreads() {
    
    let results = self.store.update { state in
      state.db.performBatchUpdates { (context) -> [EntityTable<RootState.Database.Schema, Author>.InsertionResult] in
        
        let authors = (0..<1000).map { i in
          Author(rawID: "author.\(i)")
        }
        return context.author.insert(authors)
      }
    }
    
    results.map {
      self.store.rx.nonNullEntityGetter(from: $0)
    }
    .forEach { getter in
      getter
        .do(onNext: { e in
//          print(Thread.current, e)
        })
        .subscribe()
      _ = Unmanaged.passUnretained(getter).retain()
    }
    
    let group = DispatchGroup()
        
    for _ in 0..<200 {
      group.enter()
      DispatchQueue.global().async {
        self.store.update { state in
          state.db.performBatchUpdates { (context) in
            
            let authors = (0..<1000).map { i in
              Author(rawID: "author.\(i)")
            }
            context.author.insert(authors)
          }
        }
        group.leave()
      }
    }
    
    let exp = XCTestExpectation()
    
    group.notify(queue: .main) {
      exp.fulfill()
    }
    
    wait(for: [exp], timeout: 10)
                            
  }
}
