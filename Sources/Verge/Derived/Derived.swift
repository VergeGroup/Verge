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

#if canImport(Combine)
import Combine
#endif

public protocol DerivedType<State>: StoreType {
  typealias Value = State

  func asDerived() -> Derived<Value>
}

public protocol DerivedMaking {

  associatedtype State: Equatable

  // TODO: Remove
  var state: Changes<State> { get }

  func derived<Pipeline: PipelineType>(
    _ pipeline: Pipeline,
    queue: some TargetQueueType
  ) -> Derived<Pipeline.Output> where Pipeline.Input == Changes<State>

}

/**
 A container object that provides the current value and changes from the source Store.

 This object does not know what the value managed by.
 In most cases, `Store` will be running underlying.

 Derived's functions are:
 - Computes the derived data from the state tree
 - Emit the updated data with updating Store
 - Supports subscribe the data
 - Supports Memoization

 Conforms to Equatable that compares pointer personality.
 */
public class Derived<Value: Equatable>: Store<Value, Never>, DerivedType, @unchecked Sendable {

  public override var keepsAliveForSubscribers: Bool {
    true    
  }

  /// Returns Derived object that provides constant value.
  ///
  /// - Parameter value:
  /// - Returns:
  public static func constant(_ value: Value) -> Derived<Value> {
    .init(constant: value)
  }

  fileprivate var _set: ((Value) -> Void)?
  
  private let upstreamSubscription: (any Cancellable)?
  private let retainsUpstream: Any?
  private var associatedObjects: ContiguousArray<AnyObject> = .init()

  // MARK: - Initializers

  private init(constant: Value) {
    self._set = { _ in }
    self.upstreamSubscription = nil
    self.retainsUpstream = nil
    super.init(
      name: nil,
      initialState: constant,
      storeOperation: .nonAtomic,
      logger: nil,
      sanitizer: nil
    )
  }

  /// Low-level initializer
  /// - Parameters:
  ///   - get: MemoizeMap to make a `Value` from `UpstreamState`
  ///   - set: A closure to apply new-value to `UpstreamState`, it will need in creating `BindingDerived`.
  ///   - initialUpstreamState: Initial value of the `UpstreamState`
  ///   - subscribeUpstreamState: Starts subscribe updates of the `UpstreamState`
  ///   - retainsUpstream: Any instances to retain in this instance.
  public init<UpstreamState: HasTraces, Pipeline: PipelineType>(
    name: String? = nil,
    get pipeline: Pipeline,
    set: ((Value) -> Void)?,
    initialUpstreamState: UpstreamState,
    subscribeUpstreamState: (@escaping (UpstreamState) -> Void) -> any Cancellable,
    retainsUpstream: Any?
  ) where Pipeline.Input == UpstreamState, Value == Pipeline.Output {

    let pipelineStorage: Pipeline.Storage = pipeline.makeStorage()

    weak var indirectSelf: Derived<Value>?

    let s = subscribeUpstreamState { value in
      let update = pipeline.yieldContinuously(value, storage: pipelineStorage)
      switch update {
      case .noUpdates:
        break
      case .new(let newState):
        guard let indirectSelf = indirectSelf else {
          return
        }

        // TODO: Take over state.modification & state.mutation
        indirectSelf._receive {
          $1.isDerivedFromUpstream = true
          $0.append(traces: value.traces)
          $0.replace(with: newState)
        }

      }
    }
        
    self.retainsUpstream = retainsUpstream
    self.upstreamSubscription = s
    self._set = set
    super.init(
      name: name,
      initialState: pipeline.yield(initialUpstreamState, storage: pipelineStorage),
      logger: nil,
      sanitizer: nil
    )

    indirectSelf = self

  }

  deinit {
    
  }
  
  // MARK: - Functions

  override func performInvalidation() {
    super.performInvalidation()
    self.upstreamSubscription?.cancel()
  }

  public final override func stateDidUpdate(newState: Changes<Value>) {
    // projects this update into upstream state
    if let _set, newState._transaction.isDerivedFromUpstream == false {
      _set(newState.primitive)
    }
  }
  
  public func asDerived() -> Derived<Value> {
    self
  }
  
  public func associate(_ object: AnyObject) {
    self.associatedObjects.append(object)
  }
  
  private func _combined_sinkValue(
    dropsFirst: Bool = false,
    queue: some TargetQueueType,
    receive: @escaping (Changes<Value>) -> Void
  ) -> StoreStateSubscription {
    _primitive_sinkState(
      dropsFirst: dropsFirst,
      queue: queue,
      receive: receive
    )
  }
     
  /// Make a new Derived object that projects the specified shape of the object from the object itself projects.
  ///
  /// Drops output value if no changes with Equatable
  ///
  /// - Parameters:
  ///   - queue: a queue to receive object
  ///   - pipeline:
  /// - Returns: Derived object that cached depends on the specified parameters
  /// - Attention:
  ///     As possible use the same pipeline instance and queue in order to enable caching.
  ///     Returns the Derived that previously created with that combination.
  @available(*, deprecated, renamed: "derived")
  public func chain<Pipeline: PipelineType>(
    _ pipeline: Pipeline,
    queue: some TargetQueueType = .passthrough
  ) -> Derived<Pipeline.Output> where Pipeline.Input == Changes<Value> {
    
    return derived(pipeline, queue: queue)
  }
  
}

extension Derived : Equatable {
  public static func == (lhs: Derived<Value>, rhs: Derived<Value>) -> Bool {
    lhs === rhs
  }
}

extension Derived : Hashable {
  public func hash(into hasher: inout Hasher) {
    ObjectIdentifier(self).hash(into: &hasher)
  }
}

extension Derived where Value : Equatable {
  
  /// Subscribe the state changes
  ///
  /// Receives a value only changed
  ///
  /// - Returns: A subscriber that performs the provided closure upon receiving values.
  public func sinkChangedPrimitiveValue(
    dropsFirst: Bool = false,
    queue: some TargetQueueType,
    receive: @escaping (Value) -> Void
  ) -> StoreStateSubscription {
    sinkState(dropsFirst: dropsFirst, queue: queue) { (changes) in
      changes.ifChanged().do { value in
        receive(value)
      }
    }
  }

  /// Subscribe the state changes
  ///
  /// Receives a value only changed
  ///
  /// - Returns: A subscriber that performs the provided closure upon receiving values.
  public func sinkChangedPrimitiveValue(
    dropsFirst: Bool = false,
    queue: MainActorTargetQueue = .mainIsolated(),
    receive: @escaping @MainActor (Value) -> Void
  ) -> StoreStateSubscription {
    sinkState(dropsFirst: dropsFirst, queue: queue) { @MainActor changes in
      changes.ifChanged().do { value in
        receive(value)
      }
    }
  }
  
}

// `Value == Never` eliminates specializing requirements.
extension Derived where Value == Never {
    
  /// Make Derived that projects combined value from specified source Derived objects.
  ///
  /// It retains specified Derived objects as data source until itself deallocated
  ///
  /// - Parameters:
  ///   - s0:
  ///   - s1:
  /// - Returns:
  public static func combined<S0, S1>(
    _ s0: Derived<S0>,
    _ s1: Derived<S1>,
    queue: some TargetQueueType = .passthrough
  ) -> Derived<Edge<(Changes<S0>, Changes<S1>)>> {
        
    let initial = Changes.init(old: nil, new: Edge(wrappedValue: (s0.state, s1.state)))
    
    let buffer = VergeConcurrency.RecursiveLockAtomic.init(initial)
        
    return Derived<Edge<(Changes<S0>, Changes<S1>)>>(
      get: .select({ $0 }),
      set: { _ in },
      initialUpstreamState: initial,
      subscribeUpstreamState: { callback in
                
        let _s0 = s0._combined_sinkValue(dropsFirst: true, queue: queue) { (s0) in
          buffer.modify { value in
            let newValue = value.makeNextChanges(
              with: value.primitive.next((s0, value.primitive.1)),
              from: [],
              modification: .indeterminate,
              transaction: .init()
            )
            value = newValue
            callback(newValue)
          }
        }

        let _s1 = s1._combined_sinkValue(dropsFirst: true, queue: queue) { (s1) in
          buffer.modify { value in

            let newValue = value.makeNextChanges(
              with: value.primitive.next((value.primitive.0, s1)),
              from: [],
              modification: .indeterminate,
              transaction: .init()
            )
            value = newValue
            callback(newValue)
          }
        }

        return VergeAnyCancellable(onDeinit: {
          _s0.cancel()
          _s1.cancel()
        })
        
    },
      retainsUpstream: [s0, s1]
    )
    
  }
    
  /// Make Derived that projects combined value from specified source Derived objects.
  ///
  /// It retains specified Derived objects as data source until itself deallocated
  ///
  /// - Parameters:
  ///   - s0:
  ///   - s1:
  ///   - s2:
  /// - Returns:
  public static func combined<S0, S1, S2>(
    _ s0: Derived<S0>,
    _ s1: Derived<S1>,
    _ s2: Derived<S2>,
    queue: some TargetQueueType = .passthrough
  ) -> Derived<Edge<(Changes<S0>, Changes<S1>, Changes<S2>)>> {
        
    let initial = Changes.init(old: nil, new: Edge(wrappedValue: (s0.state, s1.state, s2.state)))
    
    let buffer = VergeConcurrency.RecursiveLockAtomic.init(initial)
    
    return Derived<Edge<(Changes<S0>, Changes<S1>, Changes<S2>)>>(
      get: .select { $0 },
      set: { _ in },
      initialUpstreamState: initial,
      subscribeUpstreamState: { callback in
        
        let _s0 = s0._combined_sinkValue(dropsFirst: true, queue: queue) { (s0) in
          buffer.modify { value in
            let newValue = value.makeNextChanges(
              with: value.primitive.next((s0, value.primitive.1, value.primitive.2)),
              from: [],
              modification: .indeterminate,
              transaction: .init()
            )
            value = newValue
            callback(newValue)
          }
        }
        
        let _s1 = s1._combined_sinkValue(dropsFirst: true, queue: queue) { (s1) in
          buffer.modify { value in
            
            let newValue = value.makeNextChanges(
              with: value.primitive.next((value.primitive.0, s1, value.primitive.2)),
              from: [],
              modification: .indeterminate,
              transaction: .init()
            )
            value = newValue
            callback(newValue)
          }
        }
        
        let _s2 = s2._combined_sinkValue(dropsFirst: true, queue: queue) { (s2) in
          buffer.modify { value in
            
            let newValue = value.makeNextChanges(
              with: value.primitive.next((value.primitive.0, value.primitive.1, s2)),
              from: [],
              modification: .indeterminate,
              transaction: .init()
            )
            value = newValue
            callback(newValue)
          }
        }
        
        return VergeAnyCancellable(onDeinit: {
          _s0.cancel()
          _s1.cancel()
          _s2.cancel()
        })
        
      },
      retainsUpstream: [s0, s1, s2]
    )
    
  }
  
}

/**
 A Derived object that can set a value.
 By setting value, it forwards that value underlying the state store that providing value.
 This object does not know what the value managed by.
 In most cases, `Store` will be running underlying.
 */
@propertyWrapper
public final class BindingDerived<Value: Equatable>: Derived<Value> {
  
  /**
   Returns a derived value that created by get-pipeline.
   And can modify the value.
   
   - Warning: It does not always return the latest value after set a new value. It depends the specified target-queue.
   */
  public var wrappedValue: Value {
    get { state.primitive }
    set {
      commit {
        $0.replace(with: newValue)
      }
    }
  }

  public var projectedValue: BindingDerived<Value> {
    self
  }

}

fileprivate enum DerivedFromUpstream: TransactionKey {
  static var defaultValue: Bool { false }
}

extension Transaction {

    var isDerivedFromUpstream: Bool {
      get { self[DerivedFromUpstream.self] }
      set { self[DerivedFromUpstream.self] = newValue }
    }

}
