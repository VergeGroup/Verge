//
// Copyright (c) 2020 Hiroshi Kimura(Muukii) <muukii.app@gmail.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

import Foundation

public enum ContinuousResult<Output> {
  case new(Output)
  case noUpdates
}

extension ContinuousResult: Equatable where Output: Equatable {
  
}

/**
 A filter object that yields the output produced from the input.
 */
public protocol PipelineType<Input, Output> {
  
  associatedtype Input
  associatedtype Output
  associatedtype Storage: Sendable = Void

  func makeStorage() -> Storage

  /// Yields the output from the input.
  func yield(_ input: Input, storage: Storage) -> Output

  /// Yields the output from the input if it's needed
  func yieldContinuously(_ input: Input, storage: Storage) -> ContinuousResult<Output>

}

extension PipelineType where Storage == Void {
  public func makeStorage() -> Void {
    ()
  }
}

/**
 It produces outputs from inputs with their own conditions.
 
 Against just using map closure, it can drop output if there are no changes.
 This will be helpful in performance. Therefore most type parameters require Equatable.
 */
public enum Pipelines {

  /// KeyPath based pipeline, light weight operation just take value from source.
  public struct ChangesSelectPassthroughPipeline<Source: Equatable, Output: Equatable>: PipelineType {

    public typealias Storage = Void

    public typealias Input = Changes<Source>

    public let selector: (borrowing Input.Value) -> Output

    public init(
      selector: @escaping (borrowing Input.Value) -> Output
    ) {
      self.selector = selector
    }

    public func yieldContinuously(_ input: Input, storage: Storage) -> ContinuousResult<Output> {

      let target = input._read(perform: selector)

      return .new(consume target)

    }

    public func yield(_ input: Input, storage: Storage) -> Output {
      input._read(perform: selector)
    }

  }

  /// KeyPath based pipeline, light weight operation just take value from source.
  public struct ChangesSelectPipeline<Source: Equatable, Output: Equatable>: PipelineType {

    public typealias Storage = Void

    public typealias Input = Changes<Source>
    
    public let selector: (borrowing Input.Value) -> Output
    public let additionalDropCondition: ((Input) -> Bool)?
    
    public init(
      selector: @escaping (borrowing Input.Value) -> Output,
      additionalDropCondition: ((Input) -> Bool)?
    ) {
      self.selector = selector
      self.additionalDropCondition = additionalDropCondition
    }
    
    public func yieldContinuously(_ input: Input, storage: Storage) -> ContinuousResult<Output> {

      guard let previous = input.previous else {
        return .new(input._read(perform: selector))
      }

      let target = input._read(perform: selector)

      guard
        previous._read(perform: selector) == target
      else {
        
        guard let additionalDropCondition = additionalDropCondition, additionalDropCondition(input) else {
          return .new(consume target)
        }
        
        return .noUpdates
      }
      
      return .noUpdates
      
    }
    
    public func yield(_ input: Input, storage: Storage) -> Output {
      input._read(perform: selector)
    }
    
    public func drop(while predicate: @escaping (Input) -> Bool) -> Self {
      return .init(
        selector: selector,
        additionalDropCondition: additionalDropCondition.map { currentCondition in
          { input in
            currentCondition(input) || predicate(input)
          }
        } ?? predicate
      )
    }
          
  }
 
  /// Closure based pipeline, 
  public struct ChangesMapPipeline<Source: Equatable, Intermediate, Output: Equatable>: PipelineType {
    
    public typealias Storage = Void

    public typealias Input = Changes<Source>
    
    // MARK: - Properties
    
    public let intermediate: (Input.Value) -> PipelineIntermediate<Intermediate>
    public let transform: (Intermediate) -> Output
    public let additionalDropCondition: ((Input) -> Bool)?
    
    public init(
      @PipelineIntermediateBuilder intermediate: @escaping (Input.Value) -> PipelineIntermediate<Intermediate>,
      transform: @escaping (Intermediate) -> Output,
      additionalDropCondition: ((Input) -> Bool)?
    ) {
      self.intermediate = intermediate
      self.transform = transform
      self.additionalDropCondition = additionalDropCondition
    }
    
    // MARK: - Functions
    
    public func yieldContinuously(_ input: Input, storage: Storage) -> ContinuousResult<Output> {

      guard let previous = input.previous else {
        return .new(yield(input, storage: storage))
      }
      
      guard previous.primitive == input.primitive else {
        
        let previousIntermediate = intermediate(previous.primitive)
        let newIntermediate = intermediate(input.primitive)
        
        guard previousIntermediate == newIntermediate else {
          
          let previousMapped = transform(previousIntermediate.value)
          let newMapped = transform(newIntermediate.value)
          
          guard previousMapped == newMapped else {
            
            guard let additionalDropCondition = additionalDropCondition, additionalDropCondition(input) else {
              return .new(newMapped)
            }
            
            return .noUpdates
          }
                  
          return .noUpdates
        }
              
        return .noUpdates
        
      }
      
      return .noUpdates
    }
    
    public func yield(_ input: Input, storage: Storage) -> Output {
      transform(intermediate(input.primitive).value)
    }
      
    public func drop(while predicate: @escaping (Input) -> Bool) -> Self {
      return .init(
        intermediate: intermediate,
        transform: transform,
        additionalDropCondition: additionalDropCondition.map { currentCondition in
          { input in
            currentCondition(input) || predicate(input)
          }
        } ?? predicate
      )
    }
  }

  public struct UniqueFilterEquatable<Map: MapFunction>: PipelineType where Map.Output : Equatable {

    public typealias Input = Map.Input
    public typealias Output = Map.Output

    private let map: Map

    public init(map: Map) {
      self.map = map
    }

    public func makeStorage() -> VergeConcurrency.UnfairLockAtomic<Output?> {
      .init(nil)
    }

    public func yield(_ input: Input, storage: Storage) -> Output {
      let result = map.perform(input)
      storage.swap(result)
      return result
    }

    public func yieldContinuously(_ input: Input, storage: Storage) -> ContinuousResult<Output> {

      // not to check if input has changed because storing the input may cause performance issue by copying.

      let result = map.perform(input)

      return storage.modify { value in
        if value != result {
          value = result
          return .new(result)
        } else {
          return .noUpdates
        }
      }
    }

  }

  public struct UniqueFilter<Map: MapFunction, OutputComparator: Comparison>: PipelineType where OutputComparator.Input == Map.Output? {

    public typealias Input = Map.Input
    public typealias Output = Map.Output

    private let map: Map
    private let outputComparator: OutputComparator

    public init(map: Map, outputComparator: OutputComparator) {
      self.map = map
      self.outputComparator = outputComparator
    }

    public func makeStorage() -> VergeConcurrency.UnfairLockAtomic<Output?> {
      .init(nil)
    }

    public func yield(_ input: Input, storage: Storage) -> Output {
      let result = map.perform(input)
      storage.swap(result)
      return result
    }

    public func yieldContinuously(_ input: Input, storage: Storage) -> ContinuousResult<Output> {

      // not to check if input has changed because storing the input may cause performance issue by copying.

      let result = map.perform(input)

      return storage.modify { value in
        if !outputComparator(value, result) {
          value = result
          return .new(result)
        } else {
          return .noUpdates
        }
      }
    }

  }

}

public protocol MapFunction: Sendable {
  associatedtype Input
  associatedtype Output
  func perform(_ input: Input) -> Output
}

public struct AnyMapFunction<Input, Output>: MapFunction {

  private let _perform: @Sendable (Input) -> Output

  public init(_ perform: @escaping @Sendable (Input) -> Output) {
    self._perform = perform
  }

  public func perform(_ input: Input) -> Output {
    _perform(input)
  }
}

extension PipelineType {

  public static func uniqueMap<Map: MapFunction>(_ mapFunction: Map) -> Self
  where Map.Output: Equatable, Self == Pipelines.UniqueFilterEquatable<Map> {
    return .init(map: mapFunction)
  }

  public static func uniqueMap<Input, Output: Equatable>(_ map: @escaping @Sendable (Input) -> Output) -> Self
  where Output: Equatable, Self == Pipelines.UniqueFilterEquatable<AnyMapFunction<Input, Output>> {
    return uniqueMap(.init(map))
  }

  public static func uniqueMap<Map: MapFunction, OutputComparator: Comparison>(_ mapFunction: Map, _ outputComparator: OutputComparator) -> Self
  where Self == Pipelines.UniqueFilter<Map, OutputComparator> {
    return .init(map: mapFunction, outputComparator: outputComparator)
  }

  public static func uniqueMap<Input, Output, OutputComparator: Comparison>(_ map: @escaping @Sendable (Input) -> Output, _ outputComparator: OutputComparator) -> Self
  where Self == Pipelines.UniqueFilter<AnyMapFunction<Input, Output>, OutputComparator> {
    return .init(map: .init(map), outputComparator: outputComparator)
  }

}

extension PipelineType {

  /**
   For Changes input
   Produces output values using KeyPath-based projection.
   
   exactly same with ``PipelineType/select(_:)``
   */
  public static func map<Input, Output>(_ selector: @escaping (borrowing Input) -> Output) -> Self
  where Input: Equatable, Output: Equatable, Self == Pipelines.ChangesSelectPipeline<Input, Output> {
    select(selector)
  }
  
  /**
   For Changes input
   Produces output values using closure based projection.

   exactly same with ``PipelineType/map(_:)-7xvom``
   */
  public static func select<Input, Output>(_ selector: @escaping (borrowing Input) -> Output) -> Self
  where Input: Equatable, Output: Equatable, Self == Pipelines.ChangesSelectPipeline<Input, Output> {
    self.init(selector: selector, additionalDropCondition: nil)
  }
}

extension PipelineType {

  /**
   For Changes input
   Produces output values using closure-based projection.
   `map` closure takes the value projected from `using` closure which is intermediate value.
   If the intermediate value is not changed, map closure won't perform.
   
   - Parameters:
     - using: Specifies values for transforming. This function is annotated ``PipelineIntermediateBuilder``
     - transform: Transforms the given value from `using` function
   
   ```swift
   `.map(using: { $0.a; $0.b;}, transform: { a, b in ... }`
   ```
   
   */
  public static func map<Input, Intermediate, Output>(
    @PipelineIntermediateBuilder using intermediate: @escaping (Input) -> PipelineIntermediate<Intermediate>,
    transform: @escaping (Intermediate) -> Output
  ) -> Self where Input: Equatable, Output: Equatable, Self == Pipelines.ChangesMapPipeline<Input, Intermediate, Output> {
    
    self.init(
      intermediate: intermediate,
      transform: transform,
      additionalDropCondition: nil
    )
  }
  
  /**
   For Changes input
   Produces output values using closure-based projection.
   Using Edge as intermediate, output value will be unwrapped value from the Edge.
   */
  public static func map<Input, EdgeIntermediate>(
    @PipelineIntermediateBuilder using intermediate: @escaping (Input) -> PipelineIntermediate<Edge<EdgeIntermediate>>
  ) -> Self where Input: Equatable, Output: Equatable, Self == Pipelines.ChangesMapPipeline<Input, Edge<EdgeIntermediate>, EdgeIntermediate> {
    
    self.init(
      intermediate: intermediate,
      transform: { $0.wrappedValue },
      additionalDropCondition: nil
    )
  }
  
}

public struct PipelineIntermediate<T>: Equatable {
  
  public static func == (lhs: Self, rhs: Self) -> Bool {
    lhs.comparer(lhs.value, rhs.value)
  }
      
  public var value: T
  public let comparer: ((T, T) -> Bool)
  
  @inlinable
  public init(value: T, comparer: @escaping (T, T) -> Bool) {
    self.value = value
    self.comparer = comparer
  }
    
  @inlinable
  public init(value: T) where T: Equatable {
    self.value = value
    self.comparer = (==) // this won't be called
  }
  
}

extension PipelineIntermediate where T : Equatable {
  
  public static func == (lhs: Self, rhs: Self) -> Bool {
    lhs.value == rhs.value
  }
  
}

/**
 A result-builder that builds ``PipelineIntermediate``.
 It converts tuple into ``PipelineIntermediate`` implementing Equatable.
 
 projects:
 ```
 {
   $0.a
   $0.b
 }
 
 // alternative syntax.
 { $0.a; $0.b; }
 ```

 into:
 ```
 PipelineIntermediate<(A, B)>
 ```
 */
@resultBuilder
public enum PipelineIntermediateBuilder {
  
  public static func buildBlock<IntermediateValue>(_ i: PipelineIntermediate<IntermediateValue>) -> PipelineIntermediate<IntermediateValue> {
    return i
  }
      
  public static func buildBlock<S1: Equatable>(_ s1: S1) -> PipelineIntermediate<S1> {
    .init(value: s1)
  }
  
  public static func buildBlock<S1: Equatable, S2: Equatable>(_ s1: S1, _ s2: S2) -> PipelineIntermediate<(S1, S2)> {
    .init(value: (s1, s2), comparer: ==)
  }
  
  public static func buildBlock<S1: Equatable, S2: Equatable, S3: Equatable>(_ s1: S1, _ s2: S2, _ s3: S3) -> PipelineIntermediate<(S1, S2, S3)> {
    .init(value: (s1, s2, s3), comparer: ==)
  }
  
  public static func buildBlock<S1: Equatable, S2: Equatable, S3: Equatable, S4: Equatable>(_ s1: S1, _ s2: S2, _ s3: S3, _ s4: S4) -> PipelineIntermediate<(S1, S2, S3, S4)> {
    .init(value: (s1, s2, s3, s4), comparer: ==)
  }
  
  public static func buildBlock<S1: Equatable, S2: Equatable, S3: Equatable, S4: Equatable, S5: Equatable>(_ s1: S1, _ s2: S2, _ s3: S3, _ s4: S4, _ s5: S5) -> PipelineIntermediate<(S1, S2, S3, S4, S5)> {
    .init(value: (s1, s2, s3, s4, s5), comparer: ==)
  }
  
  public static func buildBlock<S1: Equatable, S2: Equatable, S3: Equatable, S4: Equatable, S5: Equatable, S6: Equatable>(_ s1: S1, _ s2: S2, _ s3: S3, _ s4: S4, _ s5: S5, _ s6: S6) -> PipelineIntermediate<(S1, S2, S3, S4, S5, S6)> {
    .init(value: (s1, s2, s3, s4, s5, s6), comparer: ==)
  }
}
