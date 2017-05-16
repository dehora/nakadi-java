package nakadi;

@FunctionalInterface interface EventContentSupplier {

  byte[] content();
}
