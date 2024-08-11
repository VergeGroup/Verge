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

import class Foundation.NSString

private enum CommitFromBindingDerived: TransactionKey {
  static var defaultValue: Bool { false }
}

extension Transaction {

  var isFromBindingDerived: Bool {
    get {
      self[CommitFromBindingDerived.self]
    }
    set {
      self[CommitFromBindingDerived.self] = newValue
    }
  }

}

private struct BindingDerivedPipeline<Source: Equatable, Output: Equatable, BackingPipeline: PipelineType>: PipelineType where BackingPipeline.Input == Changes<Source>, BackingPipeline.Output == Output {

  typealias Input = Changes<Source>

  private let backingPipeline: BackingPipeline

  init(backingPipeline: BackingPipeline) {
    self.backingPipeline = backingPipeline
  }

  func makeStorage() -> BackingPipeline.Storage {
    backingPipeline.makeStorage()
  }

  func yield(_ input: Changes<Source>, storage: Storage) -> Output {
    backingPipeline.yield(input, storage: storage)
  }

  func yieldContinuously(_ input: Changes<Source>, storage: Storage) -> ContinuousResult<Output> {
    if input._transaction.isFromBindingDerived {
      return .noUpdates
    }
    return backingPipeline.yieldContinuously(input, storage: storage)
  }

}

extension StoreDriverType {

  /// Returns Binding Derived object
  ///
  /// - Complexity: 💡 It's better to set `dropsOutput` predicate.
  /// - Parameters:
  ///   - name:
  ///   - get:
  ///   - dropsOutput: Predicate to drops object if found a duplicated output
  ///   - set:
  /// - Returns:
  public func bindingDerived<Pipeline: PipelineType>(
    _ name: String = "",
    _ file: StaticString = #file,
    _ function: StaticString = #function,
    _ line: UInt = #line,
    get pipeline: Pipeline,
    set: @escaping (inout InoutRef<TargetStore.State>, Pipeline.Output) -> Void,
    queue: some TargetQueueType = .passthrough
  ) -> BindingDerived<Pipeline.Output> where Pipeline.Input == Changes<TargetStore.State> {

    let derived = BindingDerived<Pipeline.Output>.init(
      get: BindingDerivedPipeline(backingPipeline: pipeline),
      set: { [weak self] state in       
        self?.store.asStore()
          ._receive {
            $1.isFromBindingDerived = true
            set(&$0, state)
          }
      },
      initialUpstreamState: store.asStore().state,
      subscribeUpstreamState: { callback in
        store.asStore()._primitive_sinkState(
          dropsFirst: true,
          queue: queue,
          receive: callback
        )
      },
      retainsUpstream: nil
    )

    store.asStore().onDeinit { [weak derived] in
      derived?.invalidate()
    }

    return derived
  }

  public func bindingDerived<Select>(
    _ name: String = "",
    _ file: StaticString = #file,
    _ function: StaticString = #function,
    _ line: UInt = #line,
    select: WritableKeyPath<TargetStore.State, Select>,
    queue: some TargetQueueType = .passthrough
  ) -> BindingDerived<Select> {

    bindingDerived(
      name, file, function, line,
      get: .select({ $0[keyPath: select] }),
      set: { state, newValue in
        state[keyPath: select] = newValue
      }
    )
  }

}

