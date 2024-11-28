//
// Copyright (c) 2019 muukii
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

import Atomics
import Combine
import Foundation
import os
import DequeModule

public final class EventEmitterCancellable: Hashable, Cancellable, @unchecked Sendable {

  public static func == (lhs: EventEmitterCancellable, rhs: EventEmitterCancellable) -> Bool {
    lhs === rhs
  }

  private weak var owner: (any EventEmitterType)?
  private(set) weak var subscriber: (any EventEmitterType)?

  fileprivate init(owner: any EventEmitterType, subscriber: (any EventEmitterType)?) {
    self.owner = owner
    self.subscriber = subscriber
  }

  public func hash(into hasher: inout Hasher) {
    ObjectIdentifier(self).hash(into: &hasher)
  }

  public func cancel() {
    owner?.removeEventHandler(self)
  }
}

public protocol EventEmitterType: AnyObject, Sendable {
  func removeEventHandler(_ token: EventEmitterCancellable)
  func _debug_subscribers() -> [any EventEmitterType]
}

public protocol EventEmitterEventType {
  func onComsume()
}

/// Instead of Combine
open class EventEmitter<Event: EventEmitterEventType>: EventEmitterType, @unchecked Sendable {

  public var publisher: Publisher {
    return .init(eventEmitter: self)
  }

  private var subscribers: VergeConcurrency.UnfairLockAtomic<[EventEmitterCancellable : (Event) -> Void]> = .init([:])

  private let queue: VergeConcurrency.UnfairLockAtomic<Deque<Event>> = .init(.init())

  private let flag = ManagedAtomic<Bool>.init(false)

  private var deinitHandlers: VergeConcurrency.UnfairLockAtomic<[() -> Void]> = .init([])

  public init() {
  }

  deinit {
    deinitHandlers.value.forEach {
      $0()
    }
  }

  @_spi(EventEmitter)
  public func accept(_ event: consuming Event) {

    /**
     https://github.com/VergeGroup/Verge/pull/220
     https://github.com/VergeGroup/Verge/issues/221
     https://github.com/VergeGroup/Verge/pull/222
     */

    // delivers a given event for subscribers at this point.
    let capturedSubscribers = subscribers.value

    queue.modify {
      $0.append(event)
    }

    if flag.compareExchange(expected: false, desired: true, ordering: .sequentiallyConsistent)
      .exchanged
    {

      while let event: Event = queue.modify({
        if $0.isEmpty == false {
          return $0.removeFirst()
        } else {
          return nil
        }
      }) {
        
        // Emits
        receiveEvent(event)
        
        for subscriber in capturedSubscribers {
          vergeSignpostEvent("EventEmitter.emitForSubscriber")
          subscriber.1(event)
        }
        event.onComsume()
      }

      /**
       might contain a bug in here?
       a conjunction of enqueue and dequeue
       */

      _ = flag.compareExchange(expected: true, desired: false, ordering: .sequentiallyConsistent)
    } else {
      // enqueue only
    }

  }
  
  open func receiveEvent(_ event: consuming Event) {

  }

  @_spi(EventEmitter)
  @discardableResult
  public func addEventHandler(subscriber: (any EventEmitterType)? = nil, eventReceiver: @escaping (Event) -> Void) -> EventEmitterCancellable {
    let token = EventEmitterCancellable(owner: self, subscriber: subscriber)
    subscribers.modify {
      $0[token] = eventReceiver
    }
    return token
  }

  public func removeEventHandler(_ token: EventEmitterCancellable) {
    var itemToRemove: ((Event) -> Void)? = nil
    subscribers.modify {
      itemToRemove = $0[token]
      $0.removeValue(forKey: token)
    }

    // To avoid triggering deinit inside removing operation
    // At this point, deallocation will happen, then ``EventEmitterCancellable` runs operations.
    // subscribers is using unfair-lock means it's not recursive lock.
    // if removes the item then deallocated inside locking, onDeinit handler runs then entering this method recursively potentially by some others.
    // then unfair-lock raises runtime error.
    withExtendedLifetime(itemToRemove, {})
  }
  
  public func onDeinit(_ onDeinit: @escaping () -> Void) {
    deinitHandlers.modify {
      $0.append(onDeinit)
    }
  }

  public func _debug_subscribers() -> [any EventEmitterType] {
    subscribers.value.keys.compactMap { $0.subscriber }
  }

  public func describeSubscriptionTree() -> String {

    return TreeRenderer.renderTree(
      root: self as EventEmitterType,
      value: { node in
        String(
          "Emitter"
        )
      },
      children: { $0._debug_subscribers() }
    )

  }

  private enum TreeRenderer {
    struct Line {
      var depth: Int
      var string: String
    }

    static func renderTree<T>(
      root: T,
      value: (T) -> String,
      children: (T) -> [T]
    ) -> String {

      var lines: [Line] = []

      func renderTree(
        root: T,
        depth: Int
      ) {

        lines.append(.init(depth: depth, string: value(root)))

        for child in children(root) {

          renderTree(root: child, depth: depth + 1)

        }

      }

      renderTree(root: root, depth: 0)

      return lines.map { line in
        Array(repeating: " ", count: line.depth * 2).joined() + "├─ " + line.string
      }
      .joined(separator: "\n")

    }

  }

}

extension EventEmitter {

  @available(iOS 13, macOS 10.15, tvOS 13, watchOS 6, *)
  public struct Publisher: Combine.Publisher {

    public typealias Output = Event

    public typealias Failure = Never

    private weak var eventEmitter: EventEmitter<Event>?

    public init(eventEmitter: EventEmitter<Event>) {
      self.eventEmitter = eventEmitter
    }

    public func receive<S>(subscriber: S)
    where S: Subscriber, Failure == S.Failure, Output == S.Input {

      let subscription = Subscription<S>(subscriber: subscriber, eventEmitter: eventEmitter)
      subscriber.receive(subscription: subscription)
    }

  }

  @available(iOS 13, macOS 10.15, tvOS 13, watchOS 6, *)
  public struct Subscription<S: Subscriber>: Combine.Subscription where S.Input == Event {

    public let combineIdentifier: CombineIdentifier = .init()

    private let subscriber: S
    private let eventEmitterSubscription: EventEmitterCancellable?
    private weak var eventEmitter: EventEmitter<Event>?

    init(subscriber: S, eventEmitter: EventEmitter<Event>?) {

      self.subscriber = subscriber
      self.eventEmitter = eventEmitter

      eventEmitter?.onDeinit {
        subscriber.receive(completion: .finished)
      }
          
      self.eventEmitterSubscription = eventEmitter?
        .addEventHandler { (event) in
          _ = subscriber.receive(event)
      }
    }

    public func request(_ demand: Subscribers.Demand) {
      // TODO: implement
    }

    public func cancel() {
      guard let eventEmitterSubscription else { return }
      eventEmitter?.removeEventHandler(eventEmitterSubscription)
    }

  }

}
