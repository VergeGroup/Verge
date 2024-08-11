
import XCTest
import Verge

final class PipelineTests: XCTestCase {
  
  func test_MapPipeline() {
    
    var mapCounter = NonAtomicCounter()
    
    let pipeline = Pipelines.ChangesMapPipeline<DemoState, _, _>(
      intermediate: {
        $0
      },
      transform: {
        mapCounter.increment()
        return $0.name.count
      },
      additionalDropCondition: nil
    )
    
    do {
      let s = DemoState()
      
      XCTAssertEqual(
        pipeline.yieldContinuously(
          Changes<DemoState>.init(
            old: s,
            new: s
          ),
          storage: ()
        ),
        .noUpdates
      )
      
      XCTAssertEqual(mapCounter.value, 0)
    }
    
    do {
      
      XCTAssertEqual(
        pipeline.yieldContinuously(
          Changes<DemoState>.init(
            old: .init(name: "A", count: 1),
            new: .init(name: "A", count: 2)
          ),
          storage: ()
        ),
        .noUpdates
      )
      
      XCTAssertEqual(mapCounter.value, 2)
      
    }
    
  }
 
  func test_MapPipeline_Intermediate() {
    
    var mapCounter = NonAtomicCounter()
    
    let pipeline = Pipelines.ChangesMapPipeline<DemoState, _, _>(
      intermediate: {
        $0.name
      },
      transform: {
        mapCounter.increment()
        return $0.count
      },
      additionalDropCondition: nil
    )
    
    do {
      let s = DemoState()
      
      XCTAssertEqual(
        pipeline.yieldContinuously(
          Changes<DemoState>.init(
            old: s,
            new: s
          ),
          storage: ()
        ),
        .noUpdates
      )
      
      XCTAssertEqual(mapCounter.value, 0)
    }
    
    do {
      
      XCTAssertEqual(
        pipeline.yieldContinuously(
          Changes<DemoState>.init(
            old: .init(name: "A", count: 1),
            new: .init(name: "A", count: 2)
          ),
          storage: ()
        ),
        .noUpdates
      )
      
      XCTAssertEqual(mapCounter.value, 0)
      
    }
        
  }
  
  func testSelect() {
    
    do {
      let store = Verge.Store<DemoState, Never>(initialState: .init())
      do {
        let d = store.derived(.map(\.$onEquatable))
        XCTAssert((d as Any) is Derived<Edge<OnEquatable>>)
      }
      
      do {
        let d = store.derived(.map(\.$nonEquatable))
        XCTAssert((d as Any) is Derived<Edge<NonEquatable>>)
      }
      
      do {
        let d = store.derived(.map(\.onEquatable))
        XCTAssert((d as Any) is Derived<OnEquatable>)
      }
            
      do {
        let d = store.derived(.map(\.count))
        XCTAssert((d as Any) is Derived<Int>)
      }
      
      do {
        let d = store.derived(.map { $0.count })
        XCTAssert((d as Any) is Derived<Int>)
      }
    }
       
  }
 
}
