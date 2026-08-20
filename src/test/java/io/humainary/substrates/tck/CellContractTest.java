// Copyright (c) 2026 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.SpecDoc;
import io.humainary.specs.api.Specs.SpecRef;
import org.junit.jupiter.api.*;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/// Conformance tests for SPEC §§11.0 and 11.2 Cell capability, seed, latest-value, publication,
/// identity, naming, and factory behavior, plus Java pipeline composition.

@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.0.0/SPEC.md")
final class CellContractTest
  extends TestSupport {

  private Cortex cortex;
  private Circuit circuit;

  /// Cell is a Substrate but not a Receptor or Pipe.
  @SpecRef({"11.0", "11.2"})
  @Test
  void cell_capabilitySurface_excludesReceptorAndPipe() {

    final Object cell =
      circuit.cell(0);

    assertInstanceOf(Substrate.class, cell);

    assertFalse(
      cell instanceof Receptor< ? >
    );

    assertFalse(
      cell instanceof Pipe< ? >
    );

  }

  /// Named Cell creation rejects required null arguments.
  @SpecRef({"11.2", "15.2"})
  @Test
  void cell_namedWithNullArguments_throwsNullPointerException() {

    assertThrows(
      NullPointerException.class,
      () -> circuit.cell(
        null,
        0
      )
    );

    assertThrows(
      NullPointerException.class,
      () -> circuit.cell(
        cortex.name("cell.null"),
        (Integer) null
      )
    );

  }

  /// Cell creation rejects an absent seed.
  @SpecRef({"11.2", "15.2"})
  @Test
  void cell_nullSeed_throwsNullPointerException() {

    assertThrows(
      NullPointerException.class,
      () -> circuit.cell(
        (Integer) null
      )
    );

    assertThrows(
      NullPointerException.class,
      () -> cortex
        .fiber(Integer.class)
        .pipe(
          (Cell< Integer >) null
        )
    );

    assertThrows(
      NullPointerException.class,
      () -> cortex
        .flow(Integer.class)
        .map(Object::toString)
        .pipe(
          (Cell< String >) null
        )
    );

  }

  /// Equal-name Cell factories return distinct handles and subjects.
  @SpecRef("11.2")
  @Test
  void cell_repeatedNamedFactory_returnsDistinctHandles() {

    final var name =
      cortex.name(
        "cell.shared"
      );

    final Cell< Integer > first =
      circuit.cell(
        name,
        0
      );

    final Cell< Integer > second =
      circuit.cell(
        name,
        0
      );

    assertNotSame(
      first,
      second
    );

    assertNotEquals(
      first.subject().id(),
      second.subject().id()
    );

    first.pipe().emit(7);
    circuit.await();

    assertEquals(
      7,
      first.get()
    );

    assertEquals(
      0,
      second.get()
    );

  }

  /// Named Cell creation binds the supplied subject name.
  @SpecRef("11.2")
  @Test
  void cell_withExplicitName_bindsSuppliedSubjectName() {

    final var name =
      cortex.name(
        "cell.named"
      );

    final Cell< Integer > cell =
      circuit.cell(
        name,
        0
      );

    assertEquals(
      name,
      cell.subject().name()
    );

    assertEquals(
      Cell.class,
      cell.subject().type()
    );

    cell.subject().enclosure(
      parent -> assertEquals(
        circuit.subject().id(),
        parent.id()
      )
    );

  }

  /// A cross-circuit update is processed by the Cell's Circuit.
  @SpecRef({"5.1", "11.2"})
  @Test
  void emit_crossCircuitSource_updatesOnCellCircuit() {

    final var owner =
      cortex.circuit();

    try {

      final var cell =
        owner.cell(
          0
        );

      final var pipe =
        cortex.fiber(
            Integer.class
          )
          .reduce(
            0,
            Integer::sum
          )
          .pipe(
            cell
          );

      pipe.emit(1);
      pipe.emit(2);

      circuit.await();
      owner.await();

      assertEquals(
        3,
        cell.get()
      );

    } finally {

      owner.close();

    }

  }

  /// Each accepted Cell pipe emission replaces the published value.
  @SpecRef("11.2")
  @Test
  void emit_intoCellPipe_replacesPublishedValue() {

    final Cell< Integer > cell =
      circuit.cell(0);

    final Pipe< Integer > pipe =
      cell.pipe();

    pipe.emit(
      7
    );

    circuit.await();

    assertEquals(
      7,
      cell.get()
    );

  }

  /// Await safely publishes an accepted Cell update to readers.
  @SpecRef({"5.4", "5.5", "11.2"})
  @Test
  void get_afterAwait_observesPublishedEmission() {

    final var cell =
      circuit.cell(
        1
      );

    assertEquals(
      1,
      cell.get()
    );

    cell.pipe().emit(
      2
    );

    circuit.await();

    assertEquals(
      2,
      cell.get()
    );

  }

  /// Cell#get returns the seed before any processed update.
  @SpecRef("11.2")
  @Test
  void get_beforeFirstEmission_returnsSeed() {

    final Cell< Integer > cell =
      circuit.cell(42);

    assertEquals(
      42,
      cell.get()
    );

  }

  /// Cell#get is total and never returns absence.
  @SpecRef("11.2")
  @Test
  void get_initializedCell_returnsNonNull() {

    final Cell< Integer > cell =
      circuit.cell(0);

    assertNotNull(
      cell.get()
    );

    cell.pipe().emit(7);
    circuit.await();

    assertNotNull(
      cell.get()
    );

  }

  /// A named Cell returns its seed then publishes a processed update.
  @SpecRef("11.2")
  @Test
  void get_namedCellBeforeAndAfterUpdate_returnsSeedThenEmission() {

    final var cell =
      circuit.cell(
        cortex.name(
          "cell.value"
        ),
        1
      );

    assertEquals(
      1,
      cell.get()
    );

    cell.pipe().emit(
      2
    );

    circuit.await();

    assertEquals(
      2,
      cell.get()
    );

  }

  /// The Cell update pipe is enclosed by the Cell subject.
  @SpecRef({"4.3", "11.0"})
  @Test
  void pipe_cellUpdateCapability_hasCellSubjectEnclosure() {

    final Cell< Integer > cell =
      circuit.cell(0);

    final Pipe< Integer > pipe =
      cell.pipe();

    assertTrue(
      pipe.subject().enclosure().isPresent()
    );

    pipe.subject().enclosure(
      parent -> assertEquals(
        cell.subject().id(),
        parent.id()
      )
    );

  }

  /// A reduced Fiber can terminate at a Cell update pipe.
  @Test
  void pipe_fiberReduction_publishesIntoCell() {

    final Cell< Integer > cell =
      circuit.cell(0);

    final var pipe =
      cortex
        .fiber(Integer.class)
        .reduce(0, Integer::sum)
        .pipe(cell);

    pipe.emit(2);
    pipe.emit(3);
    pipe.emit(5);

    circuit.await();

    assertEquals(
      10,
      cell.get()
    );

  }

  /// Cell#pipe returns one stable update capability.
  @SpecRef("11.0")
  @Test
  void pipe_repeatedLookup_returnsSameInstance() {

    final Cell< Integer > cell =
      circuit.cell(0);

    assertSame(
      cell.pipe(),
      cell.pipe()
    );

  }

  /// A Flow scan can publish immutable State accumulators into a Cell.
  @Test
  void pipe_stateFlowScan_publishesImmutableAccumulatorIntoCell() {

    record Drawdown(
      double equity,
      double peak,
      double drawdown
    ) {
    }

    final Cell< Drawdown > cell =
      circuit.cell(
        new Drawdown(1.0, 1.0, 0.0)
      );

    final var pipe =
      cortex.flow(
          Double.class
        )
        .scan(
          () -> new Drawdown(1.0, 1.0, 0.0),
          (state, value) -> {
            final var equity =
              state.equity() * (1.0 + value);

            final var peak =
              Math.max(
                state.peak(),
                equity
              );

            return
              new Drawdown(
                equity,
                peak,
                (equity / peak) - 1.0
              );
          }
        )
        .pipe(
          cell
        );

    pipe.emit(0.10);
    pipe.emit(-0.20);

    circuit.await();

    assertEquals(
      new Drawdown(0.8800000000000001, 1.1, -0.19999999999999996),
      cell.get()
    );

  }

  /// Subscriber composition can aggregate named pipes into a Cell.
  @Test
  void pipe_subscriberAggregation_publishesIntoCell() {

    final var owner =
      cortex.circuit();

    try {

      final Cell< Map< Name, Integer > > cell =
        owner.cell(
          Map.of()
        );

      final Pipe< Map< Name, Integer > > aggregate =
        cortex.< Map< Name, Integer > > flow()
          .scan(
            Map::< Name, Integer >of,
            (prior, patch) -> {
              final var entry =
                patch.entrySet().iterator().next();

              final var next =
                new HashMap<>(
                  prior
                );

              next.put(
                entry.getKey(),
                next.getOrDefault(
                  entry.getKey(),
                  0
                ) + entry.getValue()
              );

              return
                Map.copyOf(
                  next
                );
            },
            state -> state
          )
          .pipe(
            cell
          );

      final var conduit =
        circuit.conduit(
          Integer.class
        );

      conduit.subscribe(
        circuit.subscriber(
          cortex.name(
            "aggregate"
          ),
          (subject, registrar) -> {
            final Pipe< Integer > pipe =
              cortex.flow(
                  Integer.class
                )
                .map(
                  value ->
                    Map.of(
                      subject.name(),
                      value
                    )
                )
                .pipe(
                  aggregate
                );

            registrar.register(
              pipe
            );
          }
        )
      );

      final var left =
        cortex.name(
          "left"
        );

      final var right =
        cortex.name(
          "right"
        );

      conduit.get(left).emit(1);
      conduit.get(right).emit(2);
      conduit.get(left).emit(3);

      circuit.await();
      owner.await();

      assertEquals(
        Map.of(
          left,
          4,
          right,
          2
        ),
        cell.get()
      );

    } finally {

      owner.close();

    }

  }

  /// A type-changing Flow scan can publish into a Cell.
  @Test
  void pipe_typeChangingFlowScan_publishesAccumulatorIntoCell() {

    record Drawdown(
      double equity,
      double peak,
      double drawdown
    ) {
    }

    final Cell< Drawdown > cell =
      circuit.cell(
        new Drawdown(1.0, 1.0, 0.0)
      );

    final var pipe =
      cortex.flow(
          Double.class
        )
        .scan(
          () -> new Drawdown(1.0, 1.0, 0.0),
          (state, value) -> {
            final var equity =
              state.equity() * (1.0 + value);

            final var peak =
              Math.max(
                state.peak(),
                equity
              );

            return
              new Drawdown(
                equity,
                peak,
                (equity / peak) - 1.0
              );
          },
          state -> state
        )
        .pipe(
          cell
        );

    pipe.emit(0.10);
    pipe.emit(-0.20);

    circuit.await();

    assertEquals(
      new Drawdown(0.8800000000000001, 1.1, -0.19999999999999996),
      cell.get()
    );

  }

  @BeforeEach
  void setUp() {

    cortex = cortex();
    circuit = cortex.circuit();

  }

  /// A Cell subject is enclosed by its owning Circuit.
  @SpecRef({"4.3", "11.2"})
  @Test
  void subject_cell_hasCircuitEnclosure() {

    final Cell< Integer > cell =
      circuit.cell(0);

    assertNotNull(
      cell.subject()
    );

    assertEquals(
      Cell.class,
      cell.subject().type()
    );

    assertEquals(
      circuit.subject().name(),
      cell.subject().name()
    );

    assertTrue(
      cell.subject().enclosure().isPresent()
    );

    cell.subject().enclosure(
      parent -> assertEquals(
        circuit.subject().id(),
        parent.id()
      )
    );

  }

  @AfterEach
  void tearDown() {

    circuit.closeAwait();

  }

}
