import Combine
import Foundation
import SwiftUI

/**
 For SwiftUI - A View that reads a ``Store`` including ``Derived``.
 It updates its content when reading properties have been updated.
 
 Technically, it observes what properties used in making content closure as KeyPath.
 ``ReadTracker`` can get those using dynamicMemberLookup.
 Store emits events of updated state, StoreReader filters them with current using KeyPaths.
 Therefore functions of the state are not available in this situation.
 */
public struct StoreReader<StateType: Equatable, Content: View>: View {
  
  @_StateObject private var node: StoreReaderComponents<StateType>.Node
  
  public typealias ContentMaker = @MainActor (inout StoreReaderComponents<StateType>.ReadTracker) -> Content
  
  private let content: ContentMaker
  
  private init(
    node: @autoclosure @escaping () -> StoreReaderComponents<StateType>.Node,
    content: @escaping ContentMaker
  ) {
    self._node = .init(wrappedValue: node())
    self.content = content
  }
  
  public var body: some View {
    node.makeContent(content)
  }
  
  /// Initialize from `Store`
  ///
  /// - Parameters:
  ///   - store:
  ///   - content:
  public init<Store: StoreType>(
    debug: Bool = false,
    _ store: Store,
    @ViewBuilder content: @escaping ContentMaker
  ) where StateType == Store.State {
    
    let store = store.asStore()
    
    self.init(node: .init(store: store, debug: debug), content: content)
    
  }
  
  /// Creates an instance  from `Derived`
  ///
  /// - Complexity: 💡 It depends on how Derived does memoization.
  /// - Parameters:
  ///   - derived:
  ///   - content:
  public init<Derived: DerivedType>(
    debug: Bool = false,
    _ derived: Derived,
    @ViewBuilder content: @escaping ContentMaker
  ) where StateType == Derived.Value {
    
    self.init(node: .init(store: derived.asDerived().innerStore, debug: debug), content: content)
  }
}

public enum StoreReaderComponents<StateType: Equatable> {
  
  @dynamicMemberLookup
  public struct ReadTracker {
    
    private let wrapped: StateType
    
    private(set) var consumedKeyPaths: Set<PartialKeyPath<StateType>> = .init()
    
    init(wrapped: __owned StateType) {
      self.wrapped = wrapped
    }
    
    public subscript<T>(dynamicMember keyPath: KeyPath<StateType, T>) -> T {
      mutating get {
        consumedKeyPaths.insert(keyPath)
        return wrapped[keyPath: keyPath]
      }
    }
    
    public subscript<T>(dynamicMember keyPath: KeyPath<StateType, T?>) -> T? {
      mutating get {
        consumedKeyPaths.insert(keyPath)
        return wrapped[keyPath: keyPath]
      }
    }
  }
  
  @MainActor
  fileprivate final class Node: ObservableObject {
    
    nonisolated var objectWillChange: ObservableObjectPublisher {
      _publisher
    }
    
    /// nil means not loaded first yet
    private var consumingKeyPaths: Set<PartialKeyPath<StateType>>?
    
    private let _publisher: ObservableObjectPublisher = .init()
    private var cancellable: VergeAnyCancellable?
    
    private var currentValue: Changes<StateType>
    
    private let debug: Bool
    
    init<Activity>(
      store: Store<StateType, Activity>,
      debug: Bool = false
    ) {
      
      self.debug = debug
      
      self.currentValue = store.state
      
      cancellable = store.sinkState(queue: .mainIsolated()) { [weak self] state in
        
        guard let self else { return }
        
        /// retain the latest one
        self.currentValue = state
        
        /// consider to trigger update
        let shouldUpdate: Bool = {
          
          guard let consumingKeyPaths = self.consumingKeyPaths else {
            // through this filter to make content as a first time.
            return true
          }
          
          switch state.modification {
          case .determinate(let keyPaths):
            
            let hasChanges = keyPaths.intersection(consumingKeyPaths).isEmpty == false
            
            return hasChanges
          case .indeterminate:
            return true
          case nil:
            return true
          }
        }()
        
        if shouldUpdate {
          self._publisher.send()
        }
      }
      
#if DEBUG
      if debug {
        Log.debug(.storeReader, "[Node] init \(self)")
      }
#endif
      
    }
    
    deinit {
      
#if DEBUG
      if debug {
        Log.debug(.storeReader, "[Node] deinit \(self)")
      }
#endif
    }
    
    func makeContent<Content: View>(@ViewBuilder _ make: @MainActor (inout ReadTracker) -> Content)
    -> Content
    {
      var tracker = ReadTracker(wrapped: currentValue.primitive)
      let content = make(&tracker)
      
#if DEBUG
      if debug {
        
        if let consumingKeyPaths {
          
          let removedKeyPaths = consumingKeyPaths.subtracting(tracker.consumedKeyPaths)
          let addedKeyPaths = tracker.consumedKeyPaths.subtracting(consumingKeyPaths)
          
          Log.debug(
            .storeReader,
            "[MakeContent] Consumed: \(tracker.consumedKeyPaths), Removed: \(removedKeyPaths), Added: \(addedKeyPaths)"
          )
        } else {
          Log.debug(.storeReader, "[MakeContent] First load Consumed: \(tracker.consumedKeyPaths)")
        }
      }
#endif
      
      consumingKeyPaths = tracker.consumedKeyPaths
      
      return content
    }
    
  }
}


@available(iOS, deprecated: 14.0)
@propertyWrapper
private struct _StateObject<Wrapped>: DynamicProperty where Wrapped: ObservableObject {
  
  private final class Wrapper: ObservableObject {
    
    var value: Wrapped? {
      didSet {
        guard let value else { return }
        cancellable = value.objectWillChange
          .sink { [weak self] _ in
            self?.objectWillChange.send()
          }
      }
    }
    
    private var cancellable: AnyCancellable?
  }
  
  public var wrappedValue: Wrapped {
    if let object = state.value {
      return object
    } else {
      let object = thunk()
      state.value = object
      return object
    }
  }
  
  public var projectedValue: ObservedObject<Wrapped>.Wrapper {
    return ObservedObject(wrappedValue: wrappedValue).projectedValue
  }
  
  @State private var state = Wrapper()
  @ObservedObject private var observedObject = Wrapper()
  
  private let thunk: () -> Wrapped
  
  public init(wrappedValue thunk: @autoclosure @escaping () -> Wrapped) {
    self.thunk = thunk
  }
  
  public mutating func update() {
    if state.value == nil {
      state.value = thunk()
    }
    if observedObject.value !== state.value {
      observedObject.value = state.value
    }
  }
}
